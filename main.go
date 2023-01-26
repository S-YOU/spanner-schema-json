package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"cloud.google.com/go/spanner/spansql"
	"github.com/iancoleman/strcase"
	"github.com/jinzhu/copier"
	"github.com/jinzhu/inflection"
	"github.com/kenshaw/snaker"
)

var (
	ddlFile     = flag.String("ddl", "", "input ddl file")
	out         = flag.String("o", "", "output file")
	debug       = flag.Bool("debug", false, "debug")
	force       = flag.Bool("f", false, "force update")
	changedFlag = flag.Bool("changed", false, "exit code 2 if changed")
)

type TypeBase int

var baseTypes = []string{
	"bool", "int64", "float64", "int", "string", "[]byte", "civil.Date", "time.Time", "json",
}

type TypeLen int64

func (x TypeLen) MarshalJSON() ([]byte, error) {
	if x == 9223372036854775807 {
		return json.Marshal(int64(0))
	}
	return json.Marshal(int64(x))
}

type Type struct {
	Array bool     `json:"array"`
	Base  TypeBase `json:"-"`
	Len   TypeLen  `json:"len"`
}
type ColumnDef struct {
	Name           string `json:"namesDb"`
	NameDbSingular string `json:"nameDb"`
	NameJson       string `json:"nameJson"`
	NameJsonGo     string `json:"nameJsonGo"`
	GoName         string `json:"Name"`
	GoVarName      string `json:"name"`
	GoNames        string `json:"Names"`
	GoVarNames     string `json:"names"`
	NameExactJson  string `json:"nameExact"`
	NameExact      string `json:"NameExact"`
	GoType         string `json:"Type"`
	GoBaseType     string `json:"baseType"`
	Type           Type   `json:"-"`
	IsArray        bool   `json:"isArray"`
	NotNull        bool   `json:"notNull"`
	Key            string `json:"key"`
}

func lowerCamel(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}

var shortNameRe = regexp.MustCompile("[A-Z]")

func shortName(s string) string {
	return strings.ToLower(strings.Join(shortNameRe.FindAllString(s, -1), ""))
}
func (x ColumnDef) MarshalJSON() ([]byte, error) {
	x.NameExactJson = lowerCamel(x.Name)
	x.NameExact = snaker.SnakeToCamel(x.NameExactJson)
	x.NameDbSingular = inflection.Singular(x.Name)
	x.GoName = snaker.SnakeToCamel(x.NameDbSingular)
	x.GoVarName = lowerCamel(x.GoName)
	x.NameJson = strcase.ToLowerCamel(x.Name)
	x.GoNames = snaker.SnakeToCamel(plural(x.Name))
	if strings.HasSuffix(x.GoNames, "ids") {
		x.GoNames = x.GoNames[:len(x.GoNames)-3] + "Ids"
	}
	x.GoVarNames = strcase.ToLowerCamel(x.GoNames)
	if x.NameJson != "id" && strings.HasSuffix(x.NameJson, "id") {
		x.NameJson = x.NameJson[:len(x.NameJson)-2] + "Id"
	}
	x.NameJsonGo = ToGo(x.NameJson)
	x.Key = x.NameJson
	x.GoType = baseTypes[x.Type.Base]
	x.IsArray = x.Type.Array
	if !x.NotNull {
		switch x.GoType {
		case "string":
			if !x.IsArray {
				x.GoType = "spanner.NullString"
			}
		case "int64":
			x.GoType = "spanner.NullInt64"
		case "bool":
			x.GoType = "spanner.NullBool"
		case "time.Time":
			x.GoType = "spanner.NullTime"
		case "json":
			x.GoType = "spanner.NullJSON"
		default:
			x.GoType = "*" + x.GoType
		}
	}
	x.GoBaseType = x.GoType
	if x.Type.Array {
		x.GoType = "[]" + x.GoType
	}
	type MyColumnDef ColumnDef
	return json.Marshal(MyColumnDef(x))
}

type OnDelete int

const (
	NoActionOnDelete OnDelete = iota
	CascadeOnDelete
)

type Interleave struct {
	Parent   string   `json:"string"`
	OnDelete OnDelete `json:"onDelete"`
}
type KeyPart struct {
	Column         string `json:"namesDb"`
	NameDbSingular string `json:"nameDb"`
	GoName         string `json:"Name"`
	GoVarName      string `json:"name"`
	GoNames        string `json:"Names"`
	GoVarNames     string `json:"names"`
	GoType         string `json:"Type"`
	GoBaseType     string `json:"baseType"`
}

func (x KeyPart) MarshalJSON() ([]byte, error) {
	x.NameDbSingular = inflection.Singular(x.Column)
	x.GoName = snaker.SnakeToCamel(x.NameDbSingular)
	x.GoVarName = lowerCamel(x.GoName)
	x.GoNames = snaker.SnakeToCamel(plural(x.Column))
	x.GoVarNames = lowerCamel(x.GoNames)
	type MyKeyPart KeyPart
	return json.Marshal(MyKeyPart(x))
}

type TableConstraint struct {
	Name       string
	ForeignKey ForeignKey
}
type ForeignKey struct {
	Columns    []string
	RefTable   string
	RefColumns []string
}
type Table struct {
	Kind            string              `json:"kind,omitempty"`
	Name            string              `json:"namesDb"`
	NameDbSingular  string              `json:"nameDb"`
	GoName          string              `json:"Name"`
	GoVarName       string              `json:"name"`
	GoNames         string              `json:"Names"`
	GoVarNames      string              `json:"names"`
	GoShortName     string              `json:"n"`
	Key             string              `json:"key"`
	Columns         []*ColumnDef        `json:"fields"`
	PrimaryKey      []*KeyPart          `json:"primaryKey,omitempty"`
	Interleave      *Interleave         `json:"interleave,omitempty"`
	Indexes         []*CreateIndex      `json:"indexes,omitempty"`
	Constraints     []TableConstraint   `json:"-"`
	Children        []string            `json:"children,omitempty"`
	RefTables       []string            `json:"refTables,omitempty"`
	Descendents     map[string]struct{} `json:"descendents,omitempty"`
	DependencyOrder int                 `json:"dependencyOrder"`
}

func (x Table) MarshalJSON() ([]byte, error) {
	x.GoName = snaker.SnakeToCamel(x.NameDbSingular)
	x.GoVarName = lowerCamel(x.GoName)
	x.GoNames = snaker.SnakeToCamel(plural(x.Name))
	x.GoVarNames = lowerCamel(x.GoNames)
	x.GoShortName = shortName(x.GoName)
	type MyTable Table
	return json.Marshal(MyTable(x))
}

type CreateIndex struct {
	Name         string     `json:"name"`
	Table        string     `json:"table"`
	Columns      []*KeyPart `json:"fields"`
	Unique       bool       `json:"unique,omitempty"`
	NullFiltered bool       `json:"nullFiltered,omitempty"`
	WatchAll     bool       `json:"watchAll,omitempty"`
	Storing      []string   `json:"storing,omitempty"`
	Interleave   string     `json:"interleave,omitempty"`
}

func IDToString(ids []spansql.ID) []string {
	ret := make([]string, len(ids))
	for i, x := range ids {
		ret[i] = string(x)
	}
	return ret
}
func parseDDL(schema string) ([]*Table, error) {
	ddl, err := spansql.ParseDDL("", schema)
	if err != nil {
		return nil, err
	}
	tblMap := make(map[string]*Table, len(ddl.List))
	tables := make([]*Table, 0, len(ddl.List))
	colMap := make(map[string]map[string]*ColumnDef)
	keyPartMap := make(map[string]map[string]*KeyPart)
	for _, l := range ddl.List {
		switch v := l.(type) {
		case *spansql.CreateTable:
			tbl := &Table{Indexes: []*CreateIndex{}}
			if err := copier.Copy(tbl, v); err != nil {
				return nil, err
			}
			tbl.NameDbSingular = inflection.Singular(tbl.Name)
			tbl.Key = snaker.ForceCamelIdentifier(tbl.NameDbSingular)
			name := string(v.Name)
			colMap[name] = make(map[string]*ColumnDef)
			for _, c := range tbl.Columns {
				colMap[name][c.Name] = c
			}
			for _, p := range tbl.PrimaryKey {
				if c, ok := colMap[name][p.Column]; ok {
					p.GoType = baseTypes[c.Type.Base]
				} else {
					log.Println("not found", p.Column)
				}
			}
			tbl.Constraints = make([]TableConstraint, 0, len(v.Constraints))
			for _, c := range v.Constraints {
				if fk, ok := c.Constraint.(spansql.ForeignKey); ok {
					tbl.Constraints = append(tbl.Constraints, TableConstraint{
						Name: string(c.Name),
						ForeignKey: ForeignKey{
							Columns:    IDToString(fk.Columns),
							RefTable:   string(fk.RefTable),
							RefColumns: IDToString(fk.RefColumns),
						},
					})
				}
			}
			tables = append(tables, tbl)
			tblMap[name] = tbl
		case *spansql.CreateIndex:
		case *spansql.CreateChangeStream:
			tbl := &Table{Kind: "ChangeStream", Indexes: []*CreateIndex{}, Columns: []*ColumnDef{}}
			name := string(v.Name)
			tbl.Key = snaker.ForceCamelIdentifier(name)
			tbl.NameDbSingular = snaker.CamelToSnake(tbl.Key)
			tbl.Name = plural(tbl.NameDbSingular)
			keyPartMap[name] = make(map[string]*KeyPart)
			for _, c := range tbl.Columns {
				colMap[name][c.Name] = c
			}
			for _, w := range v.Watch {
				cols := make([]*KeyPart, len(w.Columns))
				for i, x := range w.Columns {
					colName := x.SQL()
					cols[i] = &KeyPart{
						Column: colName,
					}
					keyPartMap[name][colName] = cols[i]
				}
				idx := &CreateIndex{Table: w.Table.SQL(), Columns: cols, WatchAll: w.WatchAllCols}
				tbl.Indexes = append(tbl.Indexes, idx)
			}
			tables = append(tables, tbl)
			tblMap[name] = tbl
		default:
			log.Printf("unknown ddl type: %v\n", reflect.TypeOf(l))
		}
	}
	for _, l := range ddl.List {
		switch v := l.(type) {
		case *spansql.CreateTable:
		case *spansql.CreateIndex:
			table := string(v.Table)
			if t, ok := tblMap[table]; ok {
				idx := &CreateIndex{}
				if err := copier.Copy(idx, v); err != nil {
					return nil, err
				}
				for _, p := range idx.Columns {
					if c, ok := colMap[t.Name][p.Column]; ok {
						p.GoType = baseTypes[c.Type.Base]
					} else {
						log.Println("index column not found", p.Column)
					}
				}
				t.Indexes = append(t.Indexes, idx)
			}
		case *spansql.CreateChangeStream:
			name := string(v.Name)
			if ccTable, ok := tblMap[name]; ok {
				for _, w := range v.Watch {
					table := string(w.Table)
					if t, ok := tblMap[table]; ok {
						for _, ix := range ccTable.Indexes {
							for _, cx := range ix.Columns {
								if c, ok := colMap[t.Name][cx.Column]; ok {
									if p, ok := keyPartMap[name][cx.Column]; ok {
										p.GoType = baseTypes[c.Type.Base]
									}
								}
							}
						}
					}
				}
			}
		default:
			log.Printf("unknown ddl type: %v\n", reflect.TypeOf(l))
		}
	}
	return tables, nil
}

type FileContent struct {
	FileKind string   `json:"kind"`
	SrcKind  string   `json:"srcKind"`
	Data     []*Table `json:"data"`
}

func collectDescendents(keys map[string]struct{}, m map[string]*Table, out *Table) {
	for k, _ := range keys {
		if out != nil {
			for x, _ := range m[k].Descendents {
				out.Descendents[x] = struct{}{}
			}
		}
		collectDescendents(m[k].Descendents, m, m[k])
	}
}
func mtime(name string) int {
	if st, err := os.Stat(name); err != nil {
		return -1
	} else {
		return int(st.ModTime().UnixMicro())
	}
}
func process() error {
	if !*force && *out != "" && *out != "-" {
		inTime, outTime := 0, 0
		if t := mtime(*out); t > 0 && t > outTime {
			outTime = t
		}
		if outTime > 0 {
			if t := mtime(*ddlFile); t > inTime {
				inTime = t
			}
			if exe, err := os.Executable(); err == nil {
				if t := mtime(exe); t > inTime {
					inTime = t
				}
			}
			if inTime > 0 && inTime <= outTime {
				if *debug {
					log.Println("skip since no file has changed or use -f")
				}
				return nil
			}
		}
	}
	b, err := os.ReadFile(*ddlFile)
	if err != nil {
		return err
	}
	parsed, err := parseDDL(string(b))
	if err != nil {
		return err
	}
	nameMap := make(map[string]*Table, len(parsed))
	for _, v := range parsed {
		nameMap[v.Name] = v
	}
	type pair struct {
		a, b string
	}
	added := map[pair]struct{}{}
	for _, v := range parsed {
		if v.Interleave != nil {
			nameMap[v.Interleave.Parent].Children = append(nameMap[v.Interleave.Parent].Children, v.Key)
		}
		for _, vv := range v.Constraints {
			if _, ok := added[pair{vv.ForeignKey.RefTable, v.Key}]; !ok {
				if _, ok := nameMap[vv.ForeignKey.RefTable]; ok {
					nameMap[vv.ForeignKey.RefTable].RefTables = append(nameMap[vv.ForeignKey.RefTable].RefTables, v.Key)
					added[pair{vv.ForeignKey.RefTable, v.Key}] = struct{}{}
				}
			}
		}
	}
	for _, v := range parsed {
		sort.Strings(v.Children)
		sort.Strings(v.RefTables)
	}
	for _, x := range parsed {
		if x.Descendents == nil {
			x.Descendents = make(map[string]struct{})
		}
		for _, v := range x.Children {
			x.Descendents[v] = struct{}{}
		}
		for _, v := range x.RefTables {
			x.Descendents[v] = struct{}{}
		}
	}
	keys := make(map[string]struct{}, len(parsed))
	for _, x := range parsed {
		keys[x.Key] = struct{}{}
	}
	parsedMap := map[string]*Table{}
	for _, x := range parsed {
		parsedMap[x.Key] = x
	}
	collectDescendents(keys, parsedMap, nil)
	sort.SliceStable(parsed, func(i, j int) bool {
		_, ok1 := parsed[j].Descendents[parsed[i].Key]
		_, ok2 := parsed[i].Descendents[parsed[j].Key]
		if ok1 != ok2 {
			return ok1
		}
		if len(parsed[i].Descendents) != len(parsed[j].Descendents) {
			return len(parsed[i].Descendents) < len(parsed[j].Descendents)
		}
		return parsed[i].Key < parsed[j].Key
	})
	for i, x := range parsed {
		x.DependencyOrder = i + 1
	}
	fileContent := FileContent{
		FileKind: "spanner",
		SrcKind:  "spanner",
		Data:     parsed,
	}
	parsedJson, err := json.MarshalIndent(fileContent, "", "\t")
	if err != nil {
		return err
	}
	if *out == "-" {
		if _, err := os.Stdout.Write(parsedJson); err != nil {
			return err
		}
	} else {
		outFile := *out
		if outFile == "" {
			outFile = strings.Replace(*ddlFile, ".sql", ".json", 1)
		}
		if err := ioutil.WriteFile(outFile, parsedJson, 0644); err != nil {
			return err
		}
	}
	if *changedFlag {
		os.Exit(2)
	}
	return nil
}
func plural(s string) string {
	out := inflection.Plural(s)
	if out == "information" {
		return "informations"
	} else if out == "Information" {
		return "Informations"
	}
	return out
}
func main() {
	flag.Parse()
	if err := process(); err != nil {
		log.Fatalln(err)
	}
}
