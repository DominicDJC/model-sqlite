from __future__ import annotations

from enum import Enum
import sqlite3, json, types, copy
from typing import Generic, TypeVar, get_origin, get_args, Union, get_type_hints

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
        

class AttrObj(dict):
    def __getattr__(self, key):
        return self[key]
    
    def __setattr__(self, key, value):
        self[key] = value


class TrackedObj:
    def __init__(self, obj: AttrObj) -> None:
        self.obj: AttrObj = obj
        self.base: AttrObj = {}
        for key, value in obj.items():
            if type(value) == dict or type(value) == list:
                self.base[key] = copy.deepcopy(value)
            else:
                self.base[key] = value
    
    @property
    def changed(self) -> bool:
        return self.obj != self.base
    
    def get_changes(self) -> dict:
        changes: dict = {}
        for key, value in self.obj.items():
            if key in self.base and self.base[key] != value:
                changes[key] = value
        return changes


class PrimaryKey:...


class Database:
    def __init__(self, filepath: str, check_same_thread: bool = True) -> None:
        self.filepath: str = filepath
        self.database: sqlite3.Connection = sqlite3.connect(filepath, check_same_thread=check_same_thread)
        self.cursor: sqlite3.Cursor = self.database.cursor()
    
    def execute(self, command: str, commit: bool = False, vacuum: bool = False) -> sqlite3.Cursor:
        result = self.cursor.execute(command)
        if commit:
            self.database.commit()
        if vacuum:
            self.cursor.execute("VACUUM")
        return result
    
    def create_table(self, name: str, columns: dict[str, ColumnDescription]) -> None:
        self.execute(f"CREATE TABLE {name} ({', '.join(v.sql for v in columns.values())})")

    def delete_table(self, name: str) -> None:
        self.execute(f"DROP TABLE {name}")

    def clear_table(self, name: str) -> None:
        self.execute(f"DELETE FROM {name}", True, True)
    
    def insert(self, table: str, data: list, columns: list[str] = None) -> None:
        if len(data) != len(columns):
            raise InvalidColumns("Data does not match the columns")
        command: str = f"INSERT INTO {table} "
        if columns:
            command += f"({', '.join(columns)}) "
        command += f"VALUES ({', '.join([__stringify__(d) for d in data])})"
        self.execute(command, True)
    
    def table_exists(self, table: str) -> bool:
        return len(self.execute(f"SELECT name FROM sqlite_master WHERE name = '{table}'").fetchall()) > 0
    
    def get_table_columns(self, table: str) -> list[tuple]:
        return self.execute(f"PRAGMA table_info({table})").fetchall()
    
    def add_column(self, table: str, column: ColumnDescription) -> None:
        self.execute(f"ALTER TABLE {table} ADD {column.sql}", True)
    
    def delete_column(self, table: str, column: str) -> None:
        self.execute(f"ALTER TABLE {table} DROP {column}", True)
    

class Table(Generic[T]):
    def __init__(self, database: Database, name: str, model: type, dont_force_compatibility: bool = False) -> None:
        self.__loaded__: list[TrackedObj] = []
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
        return len(self.SELECT().TO_LIST()) == 0
    
    def clear(self) -> None:
        self.__database__.clear_table(self.name)

    def SELECT(self) -> Select[T]:
        return Select(self, self.__column_descriptions__, f"SELECT * FROM [{self.name}] AS [t0]")
    
    def INSERT(self, object: T) -> T:
        data: list = []
        columns: list[str] = []
        retrieval: Select = self.SELECT()
        for column, description in self.__column_descriptions__.items():
            if description.primary_key:
                retrieval = retrieval.ORDER_BY(column, True).LIMIT(1)
            if hasattr(object, column):
                value = getattr(object, column)
                if not __validate_type__(description.type, type(value)):
                    continue
                data.append(value)
                columns.append(column)
        self.__database__.insert(self.name, data, columns)
        return retrieval.TO_LIST()[-1]
    
    def UPDATE(self, object) -> Whereable[T]:
        sql: str = f"UPDATE {self.name} SET "
        for key, value in object.items() if type(object) == dict else vars(object).items():
            sql += f"'{key}' = {__stringify__(value)}, "
        return Whereable(self, self.__column_descriptions__, sql.removesuffix(", "))
    
    def save_changes(self) -> None:
        for tracked in self.__loaded__:
            if tracked.changed:
                self.UPDATE(tracked.get_changes()).WHERE_OBJ(tracked.obj).EXECUTE()


class SQLBase(Generic[T]):
    def __init__(self, table: Table, column_descriptions: dict[str, ColumnDescription], query: str = "", group: str = "", table_number: int = 0) -> None:
        self.__table__: Table = table
        self.__column_descriptions__: dict[str, ColumnDescription] = column_descriptions
        self.__query__: str = query
        self.__group__: str = group
        self.__table_number__: int = table_number
    
    @property
    def query(self) -> str:
        return f"{self.__query__}{f' {self.__group__.strip()}' if self.__group__ != '' else ''}"
    
    def EXECUTE(self) -> sqlite3.Cursor:
        return self.__table__.__database__.execute(self.query)
    
    def __append__(self, string: str) -> None:
        self.__handle_group__()
        self.__query__ += f" {string}"
    
    def __append_to_group__(self, string: str) -> None:
        self.__group__ += f" {string}"
            
    def __handle_group__(self) -> None:
        if self.__group__ != "":
            self.__query__ += f" {self.__group__.strip()}"
            self.__group__ = ""


class SQLExtension:
    def __init__(self, sql: SQLBase) -> None:
        self.__sql__ = sql
    
    @property
    def query(self) -> str:
        return self.__sql__.query


class Listable(SQLBase[T], Generic[T]):
    def TO_LIST(self) -> list[T]:
        result: list[tuple] = self.EXECUTE().fetchall()
        self.__table__.__loaded__ = []
        columns: list[str] = list(self.__column_descriptions__.keys())
        typed_result: list[T] = []
        for row in result:
            obj: AttrObj = AttrObj()
            for i in range(len(row)):
                obj[columns[i]] = self.__column_descriptions__[columns[i]].load(row[i])
            self.__table__.__loaded__.append(TrackedObj(obj))
            typed_result.append(obj)
        return typed_result
    
    def DELETE(self) -> None:
        for object in self.TO_LIST():
            self.__table__.__database__.execute(f"DELETE FROM {self.__table__.name} WHERE {__process_object__(self.__column_descriptions__, object)}", True)


class Whereable(Listable[T], Generic[T]):
    def WHERE(self) -> Operation[T]:
        self.__handle_group__()
        self.__table_number__ += 1
        self.__query__ = f"SELECT * FROM (\n\t{self.__query__}\n) AS [T{self.__table_number__}] WHERE"
        return Operation(self)
    
    def WHERE_OBJ(self, obj: T) -> Groupable[T]:
        self.__append__(" WHERE")
        self.__append_to_group__(__process_object__(self.__column_descriptions__, obj))
        return Groupable(self)
    

class Select(Whereable[T], Generic[T]):
    def DISTINCT(self) -> Select[T]:
        self.__handle_group__()
        self.__query__ = "SELECT DISTINCT" + self.__query__.removeprefix("SELECT") 
        return self
    
    def ORDER_BY(self, column: str, descending: bool = False) -> Select[T]:
        self.__handle_group__()
        self.__table_number__ += 1
        self.__query__ = f"SELECT * FROM (\n\t{self.__query__}\n) AS [T{self.__table_number__}] ORDER BY [T{self.__table_number__}].[{column}] {"DESC" if descending else "ASC"}"
        return self
    
    def LIMIT(self, number: int) -> Limited[T]:
        self.__handle_group__()
        self.__table_number__ += 1
        self.__query__ = f"SELECT * FROM (\n\t{self.__query__}\n) AS [T{self.__table_number__}] LIMIT {number}"
        return Limited(self)


class Operation(SQLExtension, Generic[T]):
    def COLUMN(self, name: str) -> LeftOperand[T]:
        self.__sql__.__append_to_group__(f"[T{self.__sql__.__table_number__}].[{name}]")
        return LeftOperand(self.__sql__)
    
    def VALUE(self, value) -> LeftOperand[T]:
        self.__sql__.__append_to_group__(__stringify__(value))
        return LeftOperand(self.__sql__)
    
    def NULL(self) -> LeftOperand[T]:
        self.__sql__.__append_to_group__("NULL")
        return LeftOperand(self.__sql__)

    def NOT(self) -> Operation[T]:
        self.__sql__.__append_to_group__("NOT")
        return Operation(self)


class LeftOperand(SQLExtension, Generic[T]):
    def EQUALS(self) -> Operator[T]:
        self.__sql__.__append_to_group__("=")
        return Operator(self.__sql__)
    
    def NOT_EQUALS(self) -> Operator[T]:
        self.__sql__.__append_to_group__("<>")
        return Operator(self.__sql__)
    
    def LESS_THAN(self) -> Operator[T]:
        self.__sql__.__append_to_group__("<")
        return Operator(self.__sql__)
    
    def LESS_THAN_EQUALS(self) -> Operator[T]:
        self.__sql__.__append_to_group__("<=")
        return Operator(self.__sql__)
    
    def GREATER_THAN(self) -> Operator[T]:
        self.__sql__.__append_to_group__(">")
        return Operator(self.__sql__)
    
    def GREATER_THAN_EQUALS(self) -> Operator[T]:
        self.__sql__.__append_to_group__(">=")
        return Operator(self.__sql__)

    def IN(self) -> Operator[T]:
        self.__sql__.__append_to_group__("IN")
        return Operator(self.__sql__)

    def NOT(self) -> LeftOperand[T]:
        self.__sql__.__append_to_group__("NOT")
        return LeftOperand(self)


class Operator(SQLExtension, Generic[T]):
    def COLUMN(self, name: str) -> Groupable[T]:
        self.__sql__.__append_to_group__(name)
        return Groupable(self.__sql__)
    
    def VALUE(self, value) -> Groupable[T]:
        self.__sql__.__append_to_group__(__stringify__(value))
        return Groupable(self.__sql__)
    
    def NULL(self) -> LeftOperand[T]:
        self.__sql__.__append_to_group__("NULL")
        return LeftOperand(self.__sql__)


class Groupable(Listable[T], Generic[T]):
    def __init__(self, sql: Listable) -> None:
        super().__init__(sql.__table__, sql.__column_descriptions__, sql.__query__, sql.__group__, sql.__table_number__)
    
    def GROUP(self) -> Groupable[T]:
        self.__append__(f"({self.__group__.strip()})")
        self.__group__ = ""
        return self
    
    def AND(self) -> Operation[T]:
        self.__append_to_group__("AND")
        return Operation(self)
    
    def OR(self) -> Operation[T]:
        self.__append_to_group__("OR")
        return Operation(self)


class Limited(Listable[T], Generic[T]):
    def __init__(self, sql: SQLBase) -> None:
        super().__init__(sql.__table__, sql.__column_descriptions__, sql.__query__)
        self.__group__ = sql.__group__
        self.__table_number__ = sql.__table_number__
    
    def offset(self, number: int) -> SQLBase[T]:
        self.__append__(f"OFFSET {number}")
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
    for key, value in get_type_hints(cls).items():
        column_descriptions[key] = ColumnDescription(
            key,
            get_args(value) if get_origin(value) in (Union, types.UnionType) else value,
            class_vars[key] if key in class_vars else None
        )
    return column_descriptions

def __process_object__(descriptions: dict[str, ColumnDescription], obj: AttrObj) -> str:
    sql: str = ""
    for key, value in obj.items():
        if key in descriptions:
            if descriptions[key].primary_key:
                return f"({key} = {__stringify__(value)})"
            sql += f" AND {key} = {__stringify__(value)}"
    return f"({sql.removeprefix(" AND ")})"