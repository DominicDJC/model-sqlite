from newsystem import Database, Table, PrimaryKey



class Message:
    id: int | PrimaryKey = None
    message: str = "Enter a message! Maybe say 'Hello, how are you today?'"
    attributes: dict = {}
    creator: str | None = None
    viewers: list[str] = []

class MessageObj:
    def __init__(self, message: str, attributes: dict = {}, creator: str | None = None, viewers: list[str] = []) -> None:
        self.id: int = None
        self.message: str = message
        self.attributes: dict = attributes
        self.creator: str | None = creator
        self.viewers: list[str] = viewers
    
    def from_message(message: Message) -> MessageObj:
        msg: MessageObj = MessageObj(message.message)
        msg.id = message.id
        msg.attributes = message.attributes
        msg.creator = message.creator
        msg.viewers = message.viewers
        return msg


def test_model_sqlite():
    # Create database and table
    # Ensure that it is empty
    database: Database = Database("test.db")
    table: Table = Table(database, "messages", Message)
    assert table.select() == []
    # Insert a row into the table
    # Ensure that it matches
    message: MessageObj = MessageObj("Test", {"readonly": True, "edits": 3}, None, ["one", "two"])
    table.insert(message)
    select: list[Message] = table.select().to_list()
    assert len(select) == 1
    assert select[0].id == 1
    assert select[0].message == message.message
    assert select[0].attributes == message.attributes
    assert select[0].creator == message.creator
    assert select[0].viewers == message.viewers
    # Reload database and table, to ensure proper loading of an existing table
    database = None
    table = None
    database = Database("test.db")
    table: Table = Table(database, "messages", Message)
    select = table.select().to_list()
    assert len(select) == 1
    assert select[0].id == 1
    assert select[0].message == message.message
    assert select[0].attributes == message.attributes
    assert select[0].creator == message.creator
    assert select[0].viewers == message.viewers
    # Updated existing row in database
    # Ensure that the row updates
    updatedMessage: Message = select[0]
    updatedMessage.message = "Test 'test'"
    updatedMessage.attributes["edits"] = 5
    updatedMessage.creator = "Sir. Tests-a-lot"
    updatedMessage.viewers.append("three")
    table.update(updatedMessage)
    select = table.select().to_list()
    assert len(select) == 1
    assert select[0].id == updatedMessage.id
    assert select[0].message == updatedMessage.message
    assert select[0].attributes == updatedMessage.attributes
    assert select[0].creator == updatedMessage.creator
    assert select[0].viewers == updatedMessage.viewers
    # Delete value from database
    # Ensure that it is deleted
    deleting: Message = Message()
    deleting.id = 1
    table.delete(deleting)
    select = table.select().to_list()
    assert len(select) == 0
    # Dealing with multiple values
    messages: list[Message] = []
    messages.append(MessageObj("First is the worst", {"outer": {"inner": [1, 2, 3]}}, "Child", []))
    messages.append(MessageObj("Second is the best", {}, "Child"))
    messages.append(MessageObj("Third is the one with the treasure chest"))
    messages.append(Message())
    for message in messages:
        table.insert(message)
    select = table.select().to_list()
    assert len(select) == 4
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == messages[i].creator
        assert select[i].viewers == messages[i].viewers
    # Advanced selecting

    # # Select single column
    # select = table.select(["message"])
    # default: Message = Message()
    # for i in range(len(select)):
    #     assert select[i].id == default.id
    #     assert select[i].message == messages[i].message
    #     assert select[i].attributes == default.attributes
    #     assert select[i].creator == default.creator
    #     assert select[i].viewers == default.viewers

    # Select with where, one statements
    select = table.select().where().column('creator').equals().value(messages[1].creator).to_list()
    # statementList: StatementList = StatementList()
    # statementList.append(Statement(Column("creator"), Operator.EQUAL, messages[1].creator))
    # select = table.select(where=statementList)
    assert len(select) == 2
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == "Child"
        assert select[i].viewers == messages[i].viewers
    # Select with where, two statements
    select = table.select().where().column('creator').equals().value(messages[1].creator).AND().column('message').equals().value(messages[1].message).to_list()
    # statementList.append(Statement(Column("message"), Operator.EQUAL, messages[1].message))
    # select = table.select(where=statementList)
    assert len(select) == 1
    assert select[0].id == 2
    assert select[0].message == messages[1].message
    assert select[0].attributes == messages[1].attributes
    assert select[0].creator == messages[1].creator
    assert select[0].viewers == messages[1].viewers

    # # Select length
    # select = table.select(length=2)
    # assert len(select) == 2
    # for i in range(len(select)):
    #     assert select[i].id == i + 1
    #     assert select[i].message == messages[i].message
    #     assert select[i].attributes == messages[i].attributes
    #     assert select[i].creator == messages[i].creator
    #     assert select[i].viewers == messages[i].viewers

    # Sort ascending
    select = table.select().order_by('message').to_list()
    # select = table.select(sort_column="message", sort_order=SortOrder.ASC)
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
    select = table.select().order_by('message', true).to_list()
    # select = table.select(sort_column="message", sort_order=SortOrder.DESC)
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
