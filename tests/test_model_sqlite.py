from __future__ import annotations
from model_sqlite import Database, Table, PrimaryKey



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
    print("Create table and test empty")
    database: Database = Database("test.db")
    table: Table = Table(database, "messages", Message)
    assert table.SELECT().TO_LIST() == []
    # Insert a row into the table
    # Ensure that it matches
    print("Insert value and check")
    message: MessageObj = MessageObj("Test", {"readonly": True, "edits": 3}, None, ["one", "two"])
    table.INSERT(message)
    select: list[Message] = table.SELECT().TO_LIST()
    assert len(select) == 1
    assert select[0].id == 1
    assert select[0].message == message.message
    assert select[0].attributes == message.attributes
    assert select[0].creator == message.creator
    assert select[0].viewers == message.viewers
    # Reload database and table, to ensure proper loading of an existing table
    print("Reload table and check")
    database = None
    table = None
    database = Database("test.db")
    table: Table = Table(database, "messages", Message)
    select = table.SELECT().TO_LIST()
    assert len(select) == 1
    assert select[0].id == 1
    assert select[0].message == message.message
    assert select[0].attributes == message.attributes
    assert select[0].creator == message.creator
    assert select[0].viewers == message.viewers
    # Updated existing row in database
    # Ensure that the row updates
    print("Update and check")
    updatedMessage: Message = select[0]
    updatedMessage.message = "Test 'test'"
    updatedMessage.attributes["edits"] = 5
    updatedMessage.creator = "Sir. Tests-a-lot"
    updatedMessage.viewers.append("three")
    select[0].message = updatedMessage.message
    select[0].attributes = updatedMessage.attributes
    select[0].creator = updatedMessage.creator
    select[0].viewers = updatedMessage.viewers
    table.save_changes()
    # table.UPDATE(updatedMessage)
    select = table.SELECT().TO_LIST()
    assert len(select) == 1
    assert select[0].id == updatedMessage.id
    assert select[0].message == updatedMessage.message
    assert select[0].attributes == updatedMessage.attributes
    assert select[0].creator == updatedMessage.creator
    assert select[0].viewers == updatedMessage.viewers
    # Delete value from database
    # Ensure that it is deleted
    table.SELECT().WHERE().COLUMN("id").EQUALS().VALUE(1).DELETE()
    select = table.SELECT().TO_LIST()
    assert len(select) == 0
    # Dealing with multiple values
    messages: list[Message] = []
    messages.append(MessageObj("First is the worst", {"outer": {"inner": [1, 2, 3]}}, "Child", []))
    messages.append(MessageObj("Second is the best", {}, "Child"))
    messages.append(MessageObj("Third is the one with the treasure chest"))
    messages.append(Message())
    for message in messages:
        table.INSERT(message)
    select = table.SELECT().TO_LIST()
    assert len(select) == 4
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == messages[i].creator
        assert select[i].viewers == messages[i].viewers
    # Advanced selecting

    # Select with where, one statements
    select = table.SELECT().WHERE().COLUMN('creator').EQUALS().VALUE(messages[1].creator).TO_LIST()
    assert len(select) == 2
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == "Child"
        assert select[i].viewers == messages[i].viewers
    # Select with where, two statements
    select = table.SELECT().WHERE().COLUMN('creator').EQUALS().VALUE(messages[1].creator).AND().COLUMN('message').EQUALS().VALUE(messages[1].message).TO_LIST()
    assert len(select) == 1
    assert select[0].id == 2
    assert select[0].message == messages[1].message
    assert select[0].attributes == messages[1].attributes
    assert select[0].creator == messages[1].creator
    assert select[0].viewers == messages[1].viewers

    # # Select length
    select = table.SELECT().LIMIT(2).TO_LIST()
    assert len(select) == 2
    for i in range(len(select)):
        assert select[i].id == i + 1
        assert select[i].message == messages[i].message
        assert select[i].attributes == messages[i].attributes
        assert select[i].creator == messages[i].creator
        assert select[i].viewers == messages[i].viewers

    # Sort ascending
    select = table.SELECT().ORDER_BY('message').TO_LIST()
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
    select = table.SELECT().ORDER_BY('message', True).TO_LIST()
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
    assert table.SELECT().TO_LIST() == []
    assert table.is_empty == True
