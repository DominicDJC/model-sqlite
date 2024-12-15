from __future__ import annotations
import sqlite3, types, json
from enum import Enum
from typing import Generic, TypeVar, Union, get_origin, get_args

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
        self.sql: str = f"{name} {to_sql_type(self.type)}{' PRIMARY KEY' if self.primary_key else ''}{' NOT NULL' if self.not_null else ''}{f' DEFAULT {stringify(default)}' if self.has_default else ''}"
    
    def load(self, value):
        if self.type in [str, dict, list] and value != None:
            value = break_string(value)
            if self.type != str:
                value = json.loads(value)
        return value


class Operator(Enum):
    EQUAL = "="
    NOT_EQUAL = "!="
    GREATER = ">"
    LESS = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    NOT_LESS = "!<"
    NOT_GREATER = "!>"


class BooleanOperator(Enum):
    AND = "AND"
    OR = "OR"


class SortOrder(Enum):
    ASC = "ASC"
    DESC = "DESC"


class Statement:
    def __init__(self, left_operand: str, operator: Operator, right_operand: str) -> None:
        self.left_operand: str = left_operand
        self.operator: Operator = operator
        self.right_operand: str = right_operand
    
    def __str__(self) -> str:
        return f"{self.left_operand} {self.operator.value} {self.right_operand}"


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


class ProcessedObject:
    def __init__(self, data: list, columns: list[str], statement_list: StatementList) -> None:
        self.data: list = data
        self.columns: list[str] = columns
        self.statement_list: StatementList = statement_list


class ProcessedObj:
    def __init__(self, columns: list[str], data: list) -> None:
        for i in range(len(columns)):
            setattr(self, columns[i], data[i])


class PrimaryKey:...


class Database:
    def __init__(self, name: str, check_same_thread: bool = True) -> None:
        self.name: str = name
        self.database: sqlite3.Connection = sqlite3.connect(name, check_same_thread=check_same_thread)
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

    def delete_table(self, name: str) -> None:
        self.execute(f"DROP TABLE {name}")

    def clear_table(self, name: str) -> None:
        self.execute(f"DELETE FROM {name}", True, True)
    
    def select(self, table: str, columns: list[str] = None, length: int = None, where: StatementList = None, sort_column: str = None, sort_order: SortOrder = SortOrder.DESC) -> list[tuple]:
        command: str = "SELECT "
        if columns:
            command += f"({', '.join(columns)})"
        else:
            command += "*"
        command += f" FROM {table}"
        if where:
            command += f" WHERE {str(where)}"
        if sort_column:
            command += f" ORDER BY {sort_column} {sort_order.value}"
        if length:
            command += f" LIMI {str(length)}"
        return self.execute(command).fetchall()
    
    def insert(self, table: str, data: list, columns: list[str] = None) -> None:
        if len(data) != len(columns):
            raise InvalidColumns("Data does not match the columns")
        command: str = f"INSERT INTO {table} "
        if columns:
            command += f"({', '.join(columns)}) "
        command += f"VALUES ({', '.join([stringify(d) for d in data])})"
        self.execute(command, True)
    
    def update(self, table: str, data: list, columns: list[str], where: StatementList = None) -> None:
        if len(data) != len(columns):
            raise InvalidColumns("Data does not match the columns")
        stringed_data: list[str] = [stringify(d) for d in data]
        self.execute(f"UPDATE {table} SET {', '.join([f'{columns[i]} = {stringed_data[i]}' for i in range(len(columns))])} WHERE {str(where)}", True)

    def delete(self, table: str, where: StatementList) -> None:
        self.execute(f"DELETE FROM {table} WHERE {str(where)}", True)
    
    def table_exists(self, table: str) -> bool:
        return len(self.select("sqlite_master", ["name"], where=StatementList(Statement("name", Operator.EQUAL, stringify(table))))) > 0
    
    def get_table_columns(self, table: str) -> list[tuple]:
        return self.execute(f"PRAGMA table_info({table})").fetchall()
    
    def add_column(self, table: str, column: ColumnDescription) -> None:
        self.execute(f"ALTER TABLE {table} ADD {column.sql}", True)
    
    def delete_column(self, table: str, column: str) -> None:
        self.execute(f"ALTER TABLE {table} DROP {column}", True)


class Table(Generic[T]):
    def __init__(self, name: str, database: Database, model: type = None, force_compatibility: bool = False) -> None:
        self.name: str = name
        self._database: Database = database
        self._model: type = model
        self._column_descriptions: dict[str, ColumnDescription] = interpret_class(self.__class__)
        if not self._database.table_exists(self.name):
            self._database.create_table(self.name, self._column_descriptions)
        elif force_compatibility:
            table_columns: list[tuple] = self._database.get_table_columns(self.name)
            for row in table_columns:
                incompatible: bool = False
                if row[1] not in self._column_descriptions.keys():
                    incompatible = True
                else:
                    column: ColumnDescription = self._column_descriptions[row[1]]
                    if  row[2] != to_sql_type(column.type):
                        incompatible = True
                    elif row[3] == 0 and column.not_null:
                        incompatible = True
                    elif column.load(row[4]) != column.default:
                        incompatible = True
                    elif row[5] != 0 and not column.primary_key:
                        incompatible = True
                if incompatible:
                    self._database.delete_column(self.name, row[1])
            table_column_names: list[str] = [c[1] for c in table_columns]
            for column_name, column_obj in self._column_descriptions.items():
                if column_name not in table_column_names:
                    self._database.add_column(self.name, column_obj)
    
    @property
    def is_empty(self) -> bool:
        return self._database.select(self.name) == []

    def delete(self, object: T = None) -> None:
        self._database.delete(self.name, process_object(self._column_descriptions, object).statement_list if object else None)
    
    def clear(self) -> None:
        self._database.clear_table(self.name)

    def insert(self, object: T) -> None:
        processed: ProcessedObject = process_object(self._column_descriptions, object)
        self._database.insert(self.name, processed.data, processed.columns)

    def update(self, object: T) -> None:
        processed: ProcessedObject = process_object(self._column_descriptions, object)
        self._database.update(self.name, processed.data, processed.columns, processed.statement_list)

    def select(self, columns: list[str] = None, length: int = None, where: StatementList = None, sort_column: str = None, sort_order: SortOrder = SortOrder.DESC) -> list[T]:
        result: list[tuple] = self._database.select(self.name, columns, length, where, sort_column, sort_order)
        if result == []:
            return []
        typed_result: list[T] = []
        if not columns:
            columns = list(self._column_descriptions.keys())
        else:
            for column in columns:
                if column not in self._column_descriptions.keys():
                    raise InvalidColumns(f"Column {column} does not exist on table {self.name}")
        if len(result[0]) != len(columns):
            raise InvalidColumns("Results do not match the columns")
        for row in result:
            obj: self._model = self._model()
            for i in range(len(columns)):
                setattr(obj, columns[i], self._column_descriptions[columns[i]].load(row[i]))
            typed_result.append(obj)
        return typed_result


def interpret_class(cls: type) -> dict:
    column_descriptions: dict[str, ColumnDescription] = {}
    class_vars: dict = vars(cls)
    for key, value in cls.__annotations__.items():
        column_descriptions[key] = ColumnDescription(
            key,
            get_args(value) if get_origin(value) in (Union, types.UnionType) else value,
            class_vars[key] if key in class_vars else None
        )
    return column_descriptions

def to_sql_type(cls: type) -> str:
    if cls == int:
        return "INTEGER"
    elif cls == float:
        return "REAL"
    elif cls in [str, dict, list]:
        return "TEXT"
    return ""

def fix_string(string: str) -> str:
    string = string.replace("'", "''")
    return f"'{string}'"

def break_string(string: str) -> str:
    string = string.removeprefix("'")
    string = string.removesuffix("'")
    string = string.replace("''", "'")
    return string

def stringify(data) -> str:
    if type(data) == str:
        return fix_string(data)
    elif type(data) in [dict, list]:
        return fix_string(json.dumps(data))
    elif data == None:
        return "NULL"
    else:
        return str(data)

def process_object(column_descriptions: dict[str, ColumnDescription], object) -> ProcessedObject:
    data: list = []
    obj_columns: list[str] = []
    statement_list: StatementList = StatementList()
    pked: bool = False
    for key, value in vars(object).items():
        if key in column_descriptions and column_descriptions[key].type == type(value):
            data.append(value)
            obj_columns.append(key)
            if not pked:
                statement: Statement = StatementList(Statement(key, Operator.EQUAL, stringify(value)))
                if column_descriptions[key].primary_key:
                    statement_list = StatementList(statement)
                    pked = True
                else:
                    statement_list.append(statement)
    return ProcessedObject(data, obj_columns, statement_list)