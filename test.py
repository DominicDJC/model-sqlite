# from newsystem import Database, Table

# from typing import get_origin, Union, types, get_args, Generic, TypeVar

# T = TypeVar('T')

# class PrimaryKey:...

# class ColumnDescription:
#     def __init__(self, name: str, type_definitions: type | tuple[type], default) -> None:
#         self.type = type_definitions[0] if type(type_definitions) == tuple else type_definitions
#         self.primary_key: bool = False
#         self.not_null: bool = True
#         if type(type_definitions) == tuple:
#             if PrimaryKey in type_definitions:
#                 self.primary_key = True
#             if None in type_definitions or types.NoneType in type_definitions:
#                 self.not_null = False
#         self.has_default: bool = default != None
#         self.default = default
#         self.sql: str = f"{name} {__to_sql_type__(self.type)}{' PRIMARY KEY' if self.primary_key else ''}{' NOT NULL' if self.not_null else ''}{f' DEFAULT {__stringify__(default)}' if self.has_default else ''}"
    
#     def load(self, value, fix_string: bool = False):
#         if value != None:
#             if fix_string and self.type == str:
#                 return __break_string__(value)
#             if (self.type == dict or __is_list__(self.type)):
#                 value = __break_string__(value)
#                 value = json.loads(value)
#         return value


# class User:
#     id: int = None
#     name: str | None = ""

# class UserObj:
#     def __init__(self, user: User) -> None:
#         self.id: int = user.id
#         self.name: str = user.name


# class AttrObj(dict):
#     def __getattr__(self, key):
#         return self[key]
    
#     def __setattr__(self, key, value):
#         self[key] = value



# def __to_sql_type__(cls: type) -> str:
#     if cls == int:
#         return "INTEGER"
#     elif cls == float:
#         return "REAL"
#     elif cls in [str, dict] or __is_list__(cls):
#         return "TEXT"
#     return ""


# def __fix_string__(string: str) -> str:
#     string = string.replace("'", "''")
#     return f"'{string}'"

# def __break_string__(string: str) -> str:
#     string = string.removeprefix("'")
#     string = string.removesuffix("'")
#     string = string.replace("''", "'")
#     return string

# def __stringify__(data) -> str:
#     if type(data) == str:
#         return __fix_string__(data)
#     elif type(data) in [dict, list]:
#         return __fix_string__(json.dumps(data))
#     elif type(data) == Column:
#         return data.name
#     elif data == None:
#         return "NULL"
#     else:
#         return str(data)


# def __interpret_class__(cls: type) -> dict:
#     column_descriptions: dict[str, ColumnDescription] = {}
#     class_vars: dict = vars(cls)
#     for key, value in cls.__annotations__.items():
#         column_descriptions[key] = ColumnDescription(
#             key,
#             get_args(value) if get_origin(value) in (Union, types.UnionType) else value,
#             class_vars[key] if key in class_vars else None
#         )
#     return column_descriptions

# def test_function(model: type = None):
#     obj: AttrObj = AttrObj()
#     obj.id = 4
#     obj.name = "test"
#     if model:
#         return model(obj)
#     return obj


# # for key, value in __interpret_class__(User).items():
# #     print(key)
# #     print(vars(value))

# # print(vars(test_function(UserObj)))

# database: Database = Database("test.db")
# table: Table = Table(database, "users", User)

import tests.test_model_sqlite

tests.test_model_sqlite.test_model_sqlite()