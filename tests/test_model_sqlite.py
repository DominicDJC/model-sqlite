from model_sqlite import Database, Table, Statement, StatementList, PrimaryKey, Column, Operator, SortOrder, BooleanOperator


class MessageModel:
    def __init__(self) -> None:
        self.id: int = None
        self.message: str = "Enter a message! Maybe say 'Hello, how are you today?'"
        self.attributes: dict = {}
        self.creator: str | None = None
        self.viewers: list[str] = []


class Message(MessageModel):
    def __init__(self, message: str, attributes: dict = {}, creator: str | None = None, viewers: list[str] = []) -> None:
        super().__init__()
        self.message = message
        self.attributes = attributes
        self.creator = creator
        self.viewers = viewers


class Messages(Table[MessageModel]):
    id: int | PrimaryKey
    message: str = ""
    attributes: dict = {}
    creator: str | None = None
    viewers: list[str] = []


def test_model_sqlite():
    # Create database and table
    # Ensure that it is empty
    print("=== Creating")
    database: Database = Database("test.db")
    table: Messages = Messages("test", database, MessageModel)
    assert table.select() == []
    # Insert a row into the table
    # Ensure that it matches
    print("=== Inserting")
    message: Message = Message("Test", {"readonly": True, "edits": 3}, None, ["one", "two"])
    table.insert(message)
    select: list[MessageModel] = table.select()
    for obj in select:
        print(vars(obj))
    assert len(select) == 1
    assert select[0].id == 1
    assert select[0].message == message.message
    assert select[0].attributes == message.attributes
    assert select[0].creator == message.creator
    assert select[0].viewers == message.viewers
    # Reload database and table, to ensure proper loading of an existing table
    print("=== Reloading")
    database = None
    table = None
    database = Database("test.db", logging=True)
    table = Messages("test", database, MessageModel)
    select = table.select()
    for obj in select:
        print(vars(obj))
    assert len(select) == 1
    assert select[0].id == 1
    assert select[0].message == message.message
    assert select[0].attributes == message.attributes
    assert select[0].creator == message.creator
    assert select[0].viewers == message.viewers
    # Updated existing row in database
    # Ensure that the row updates
    print("=== Updaing")
    updatedMessage: MessageModel = select[0]
    updatedMessage.message = "Test 'test'"
    updatedMessage.attributes["edits"] = 5
    updatedMessage.creator = "Sir. Tests-a-lot"
    updatedMessage.viewers.append("three")
    table.update(updatedMessage)
    select = table.select()
    for obj in select:
        print(vars(obj))
    assert len(select) == 1
    assert select[0].id == updatedMessage.id
    assert select[0].message == updatedMessage.message
    assert select[0].attributes == updatedMessage.attributes
    assert select[0].creator == updatedMessage.creator
    assert select[0].viewers == updatedMessage.viewers
    # Delete value from database
    # Ensure that it is deleted
    print("=== Deleting")
    deleting: MessageModel = MessageModel()
    deleting.id = 1
    table.delete(deleting)
    select = table.select()
    for obj in select:
        print(vars(obj))
    assert len(select) == 0
    # Dealing with multiple values
    print("=== Multiple values")
    messages: list[MessageModel] = []
    messages.append(Message("First is the worst", {"outer": {"inner": [1, 2, 3]}}, "Child", []))
    messages.append(Message("Second is the best", {}, "Child"))
    messages.append(Message("Third is the one with the treasure chest"))
    messages.append(MessageModel())
    for message in messages:
        table.insert(message)
    select = table.select()
    for obj in select:
        print(vars(obj))
    assert len(select) == 4
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == messages[i].creator
        assert select[i].viewers == messages[i].viewers
    # Advanced selecting
    print("=== Advanced selecting")
    # Select single column
    select = table.select(["message"])
    default: MessageModel = MessageModel()
    for obj in select:
        print(vars(obj))
    for i in range(len(select)):
        assert select[i].id == default.id
        assert select[i].message == messages[i].message
        assert select[i].attributes == default.attributes
        assert select[i].creator == default.creator
        assert select[i].viewers == default.viewers
    # Select with where, one statements
    statementList: StatementList = StatementList()
    statementList.append(Statement(Column("creator"), Operator.EQUAL, messages[1].creator))
    select = table.select(where=statementList)
    for obj in select:
        print(vars(obj))
    assert len(select) == 2
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == "Child"
        assert select[i].viewers == messages[i].viewers
    # Select with where, two statements
    statementList.append(Statement(Column("message"), Operator.EQUAL, messages[1].message))
    select = table.select(where=statementList)
    for obj in select:
        print(vars(obj))
    assert len(select) == 1
    assert select[0].id == 2
    assert select[0].message == messages[1].message
    assert select[0].attributes == messages[1].attributes
    assert select[0].creator == messages[1].creator
    assert select[0].viewers == messages[1].viewers
    # Select length
    select = table.select(length=2)
    for obj in select:
        print(vars(obj))
    assert len(select) == 2
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == messages[i].creator
        assert select[i].viewers == messages[i].viewers
    # Sort ascending
    select = table.select(sort_column="message", sort_order=SortOrder.ASC)
    for obj in select:
        print(vars(obj))
    assert len(select) == 4
    for i in range(len(select)):
        j: int = select[i].id - 1
        assert select[i].message == messages[j].message
        assert select[i].attributes == messages[j].attributes
        assert select[i].creator == messages[j].creator
        assert select[i].viewers == messages[j].viewers
        k: int = i - 1
        if k > -1 and k < len(select):
            assert select[k].message < select[i].message
    # Sort descending
    select = table.select(sort_column="message", sort_order=SortOrder.DESC)
    for obj in select:
        print(vars(obj))
    assert len(select) == 4
    for i in range(len(select)):
        j: int = select[i].id - 1
        assert select[i].message == messages[j].message
        assert select[i].attributes == messages[j].attributes
        assert select[i].creator == messages[j].creator
        assert select[i].viewers == messages[j].viewers
        k: int = i - 1
        if k > -1 and k < len(select):
            assert select[k].message > select[i].message
    # Table is not empty
    assert table.is_empty == False
    # Clear
    table.clear()
    # Table is empty
    assert table.select() == []
    assert table.is_empty == True
