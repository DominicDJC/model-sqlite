from __future__ import annotations

import sqlite3, json
from typing import Generic, TypeVar, get_origin, get_args, Union, types

T = TypeVar('T')




class InvalidColumns(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)




class ColumnDescription:
    def __init__(self, name: str, type_definitions: type | tuple[type], default) -> None:
        self.type = type_definitions[0] if type(type_definitions) == tuple else type_definitions
        self.primary_key: bool = False
        self.not_null: bool = True
        if type(type_definitions) == tuple:
            if PrimaryKey in type_definitions:
                self.primary_key = True
            if None in type_definitions or types.NoneType in type_definitions:
                self.not_null = False
        self.has_default: bool = default != None
        self.default = default
        self.sql: str = f"{name} {__to_sql_type__(self.type)}{' PRIMARY KEY' if self.primary_key else ''}{' NOT NULL' if self.not_null else ''}{f' DEFAULT {__stringify__(default)}' if self.has_default else ''}"
    
    def load(self, value, fix_string: bool = False):
        if value != None:
            if fix_string and self.type == str:
                return __break_string__(value)
            if (self.type == dict or __is_list__(self.type)):
                value = __break_string__(value)
                value = json.loads(value)
        return value


class ProcessedObject:
    def __init__(self, data: list, columns: list[str], statement_list: StatementList) -> None:
        self.data: list = data
        self.columns: list[str] = columns
        self.statement_list: StatementList = statement_list
        

class AttrObj(dict):
    def __getattr__(self, key):
        return self[key]
    
    def __setattr__(self, key, value):
        self[key] = value


class Column:
    def __init__(self, name: str) -> None:
        self.name: str = name


class Statement:
    def __init__(self, left_operand, operator: Operator, right_operand) -> None:
        self.left_operand = left_operand
        self.operator: Operator = operator
        self.right_operand = right_operand
    
    def __str__(self) -> str:
        return f"{__stringify__(self.left_operand)} {self.operator.value} {__stringify__(self.right_operand)}"


class StatementList:
    def __init__(self, first_statement: Statement = None) -> None:
        self.statements: list[Statement] = []
        self.operators: list[BooleanOperator] = []
        if first_statement:
            self.statements.append(first_statement)
    
    def append(self, statement: Statement, operator: BooleanOperator = BooleanOperator.AND) -> None:
        self.statements.append(statement)
        if len(self.statements) != 0:
            self.operators.append(operator)
    
    def __str__(self) -> str:
        string: str = str(self.statements[0])
        for i in range(len(self.operators) - 1):
            string += f" {self.operators[i].value} {str(self.statements[i + 1])}"
        return string


class PrimaryKey:...


class Database:
    def __init__(self, filepath: str, check_same_thread: bool = True) -> None:
        self.filepath: str = filepath
        self.database: sqlite3.Connection = sqlite3.connect(filepath, check_same_thread=check_same_thread)
        self.cursor: sqlite3.Cursor = self.database.cursor()
    
    def execute(self, command: str, commit: bool = False, vacuum: bool = False) -> sqlite3.Cursor:
        print(command)
        result = self.cursor.execute(command)
        if commit:
            self.database.commit()
        if vacuum:
            self.cursor.execute("VACUUM")
        return result
    
    def create_table(self, name: str, columns: dict[str, ColumnDescription]) -> None:
        self.execute(f"CREATE TABLE {name} ({', '.join(v.sql for v in columns.values())})")
    
    def table_exists(self, table: str) -> bool:
        return len(self.execute(f"SELECT name FROM sqlite_master WHERE name = '{table}'").fetchall()) > 0
    
    def get_table_columns(self, table: str) -> list[tuple]:
        return self.execute(f"PRAGMA table_info({table})").fetchall()
    

class Table(Generic[T]):
    def __init__(self, database: Database, name: str, model: type, dont_force_compatibility: bool = False) -> None:
        self.__database__: Database = database
        self.name: str = name
        self.__model__: type = model
        self.__column_descriptions__: dict[str, ColumnDescription] = __interpret_class__(model)
        if not self.__database__.table_exists(self.name):
            self.__database__.create_table(self.name, self.__column_descriptions__)
        elif not dont_force_compatibility:
            table_columns: list[tuple] = self.__database__.get_table_columns(self.name)
            for row in table_columns:
                incompatible: bool = False
                if row[1] not in self.__column_descriptions__.keys():
                    incompatible = True
                else:
                    column: ColumnDescription = self.__column_descriptions__[row[1]]
                    if  row[2] != __to_sql_type__(column.type):
                        incompatible = True
                    elif row[3] == 0 and column.not_null:
                        incompatible = True
                    elif column.load(row[4], fix_string=True) != column.default:
                        incompatible = True
                    elif row[5] != 0 and not column.primary_key:
                        incompatible = True
                if incompatible:
                    self.__database__.delete_column(self.name, row[1])
            table_column_names: list[str] = [c[1] for c in table_columns]
            for column_name, column_obj in self.__column_descriptions__.items():
                if column_name not in table_column_names:
                    self.__database__.add_column(self.name, column_obj)
    
    @property
    def is_empty(self) -> bool:
        return self._database.select(self.name) == []

    def delete(self, object: T = None) -> None:
        self._database.delete(self.name, __process_object__(self._column_descriptions, object).statement_list if object else None)
    
    def clear(self) -> None:
        self._database.clear_table(self.name)

    def insert(self, object: T) -> None:
        processed: ProcessedObject = __process_object__(self._column_descriptions, object)
        self._database.insert(self.name, processed.data, processed.columns)

    def update(self, object: T) -> None:
        processed: ProcessedObject = __process_object__(self._column_descriptions, object)
        self._database.update(self.name, processed.data, processed.columns, processed.statement_list)

    def select(self) -> SQL[T]:
        return SQL[T](database, query=f"SELECT * FROM [{self.name}] AS [t0]")
    

class SQL(Generic[T]):
    def __init__(self, database: Database, column_descriptions: dict[str, ColumnDescription], query: str = "") -> None:
        self.__database__: Database = database
        self.__column_descriptions__: dict[str, ColumnDescription] = column_descriptions
        self.__query__: str = query
        self.__group__: str = ""
        self.__table_number__: int = 0
    
    @property
    def query(self) -> str:
        return f"{self.__query__}{f' {self.__group__.strip()}' if self.__group__ != '' else ''}"
    
    def __append__(self, string: str) -> None:
        self.__query__ += f" {string}"
    
    def __append_to_group__(self, string: str) -> None:
        self.__group__ += f" {string}"
    
    def distinct(self) -> SQL:
        self.__handle_group__()
        self.__query__ = "SELECT DISTINCT" + self.__query__.removeprefix("SELECT") 
        return self
    
    def where(self) -> Where:
        self.__append__("WHERE")
        return Where(self)
    
    def order_by(self, column: str, descending: bool = False) -> SQL:
        self.__handle_group__()
        self.__table_number__ += 1
        self.__query__ = f"SELECT * FROM (\n\t{self.__query__}\n) AS [t{self.__table_number__}] ORDER BY [t{self.__table_number__}].[{column}] {"DESC" if descending else "ASC"}"
        return self
    
    def to_list(self) -> list[T]:
        self.__handle_group__()
        result: list[tuple] = self.__database__.execute(self.__query__).fetchall()
        typed_result: list[T] = []
        for row in result:
            obj: AttrObj = AttrObj
            for i in range(len(row)):
                obj[self.__column_descriptions__.keys()[i]] = row[i]
            typed_result.append(obj)
        return typed_result
            
    
    def __handle_group__(self) -> None:
        if self.__group__ != "":
            self.__query__ += f" {self.__group__.strip()}"
            self.__group__ = ""


class Where:
    def __init__(self, sql: SQL) -> None:
        self.sql: SQL = sql
    
    @property
    def query(self) -> str:
        return self.sql.query
    
    def column(self, name: str) -> LeftOperand:
        self.sql.__append_to_group__(f"[t{self.sql.__table_number__}].[{name}]")
        return LeftOperand(self.sql)
    
    def value(self, value) -> LeftOperand:
        self.sql.__append_to_group__(__stringify__(value))
        return LeftOperand(self.sql)


class LeftOperand:
    def __init__(self, sql: SQL) -> None:
        self.sql: SQL = sql
    
    @property
    def query(self) -> str:
        return self.sql.query
    
    def equals(self) -> Operator:
        self.sql.__append_to_group__("=")
        return Operator(self.sql)
    
    def less_than(self) -> Operator:
        self.sql.__append_to_group__("<")
        return Operator(self.sql)
    
    def less_than_equal(self) -> Operator:
        self.sql.__append_to_group__("<=")
        return Operator(self.sql)
    
    def greater_than(self) -> Operator:
        self.sql.__append_to_group__(">")
        return Operator(self.sql)
    
    def greater_than_equal(self) -> Operator:
        self.sql.__append_to_group__(">=")
        return Operator(self.sql)


class Operator:
    def __init__(self, sql: SQL) -> None:
        self.sql: SQL = sql
    
    @property
    def query(self) -> str:
        return self.sql.query
    
    def column(self, name: str) -> Groupable:
        self.sql.__append_to_group__(name)
        return Groupable(self.sql)
    
    def value(self, value) -> Groupable:
        self.sql.__append_to_group__(__stringify__(value))
        return Groupable(self.sql)


class Groupable(SQL):
    def __init__(self, sql: SQL) -> None:
        super().__init__(sql.database, sql.__query__)
        self.__group__ = sql.__group__
        self.__table_number__ = sql.__table_number__
    
    def group(self) -> Groupable:
        self.__append__(f"({self.__group__.strip()})")
        self.__group__ = ""
        return self
    
    def AND(self) -> Where:
        self.__append_to_group__("AND")
        return Where(self)
    
    def OR(self) -> Where:
        self.__append_to_group__("OR")
        return Where(self)
    
    def where(self) -> SQL:
        print("INVALID WHERE CALL")
        return self


def __fix_string__(string: str) -> str:
    string = string.replace("'", "''")
    return f"'{string}'"

def __break_string__(string: str) -> str:
    string = string.removeprefix("'")
    string = string.removesuffix("'")
    string = string.replace("''", "'")
    return string

def __stringify__(data) -> str:
    if type(data) == str:
        return __fix_string__(data)
    elif type(data) in [dict, list]:
        return __fix_string__(json.dumps(data))
    elif data == None:
        return "NULL"
    else:
        return str(data)

def __is_list__(t: type) -> bool:
    return t == list or hasattr(t, "__origin__") and t.__origin__ == list

def __validate_type__(column: type, value: type) -> bool:
    if column == value:
        return True
    if __is_list__(column) and __is_list__(value):
        return True
    return False

def __to_sql_type__(cls: type) -> str:
    if cls == int:
        return "INTEGER"
    elif cls == float:
        return "REAL"
    elif cls in [str, dict] or __is_list__(cls):
        return "TEXT"
    return ""

def __interpret_class__(cls: type) -> dict:
    column_descriptions: dict[str, ColumnDescription] = {}
    class_vars: dict = vars(cls)
    for key, value in cls.__annotations__.items():
        column_descriptions[key] = ColumnDescription(
            key,
            get_args(value) if get_origin(value) in (Union, types.UnionType) else value,
            class_vars[key] if key in class_vars else None
        )
    return column_descriptions

def __process_object__(column_descriptions: dict[str, ColumnDescription], object) -> ProcessedObject:
    data: list = []
    obj_columns: list[str] = []
    statement_list: StatementList = StatementList()
    pked: bool = False
    for key, value in vars(object).items():
        if key in column_descriptions and __validate_type__(column_descriptions[key].type, type(value)):
            data.append(value)
            obj_columns.append(key)
            if not pked:
                statement: Statement = StatementList(Statement(Column(key), Operator.EQUAL, __stringify__(value)))
                if column_descriptions[key].primary_key:
                    statement_list = StatementList(statement)
                    pked = True
                else:
                    statement_list.append(statement)
    return ProcessedObject(data, obj_columns, statement_list)

# database: Database = Database("test.db")
# table: Table = Table(database, "test_table")

# print(table.select().where().column('test').equals().value('test').distinct().query)
