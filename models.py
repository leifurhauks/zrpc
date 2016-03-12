from goblin import properties
from goblin.models import Vertex, Edge, V
from goblin.relationships import Relationship


# Define edge models
class WorksFor(Edge):
    start_date = properties.DateTime()


class MemberOf(Edge):
    since = properties.DateTime()


class BelongsTo(Edge):
    pass


# Define vertex models
class Organization(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()


class Department(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()
    belongs_to = Relationship(BelongsTo, Organization)


class Person(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()
    works_for = Relationship(WorksFor, Organization)
    member_of = Relationship(MemberOf, Department)
