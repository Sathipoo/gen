from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Attribute:
    name: str
    value: str

@dataclass
class SourceField:
    BUSINESSNAME: Optional[str]
    DATATYPE: Optional[str]
    DESCRIPTION: Optional[str]
    FIELDATTRIBUTE: Optional[str]
    NAME: str
    NULLABLE: Optional[str]
    PICTURETEXT: Optional[str]
    PRECISION: Optional[str]
    SCALE: Optional[str]
    USAGE_FLAGS: Optional[str]

@dataclass
class FlatFile:
    CODEPAGE: Optional[str]
    CONSECDELIMITERSASONE: Optional[str]
    DELIMITED: Optional[str]
    ESCAPE_CHARACTER: Optional[str]
    FILETYPE: Optional[str]
    QUOTE_CHARACTER: Optional[str]
    REC_DELIMITERS: Optional[str]
    ROWDELIMITER: Optional[str]
    SKIPROWS: Optional[str]
    STRIPTRAILINGBLANKS: Optional[str]
    TRIM_SPACE: Optional[str]

@dataclass
class TableAttribute:
    NAME: str
    VALUE: str

@dataclass
class TargetField:
    BUSINESSNAME: Optional[str]
    DATATYPE: Optional[str]
    DESCRIPTION: Optional[str]
    FIELDATTRIBUTE: Optional[str]
    NAME: str
    NULLABLE: Optional[str]
    PICTURETEXT: Optional[str]
    PRECISION: Optional[str]
    SCALE: Optional[str]

@dataclass
class Source:
    BUSINESSNAME: Optional[str]
    DATABASETYPE: Optional[str]
    DBDNAME: Optional[str]
    DESCRIPTION: Optional[str]
    NAME: str
    OBJECTVERSION: Optional[str]
    OWNERNAME: Optional[str]
    VERSIONNUMBER: Optional[str]
    SOURCEFIELD: List[SourceField] = field(default_factory=list)

@dataclass
class Target:
    BUSINESSNAME: Optional[str]
    CONSTRAINT: Optional[str]
    DATABASETYPE: Optional[str]
    DESCRIPTION: Optional[str]
    NAME: str
    OBJECTVERSION: Optional[str]
    OWNERNAME: Optional[str]
    VERSIONNUMBER: Optional[str]
    FLATFILE: Optional[FlatFile] = None
    TARGETFIELD: List[TargetField] = field(default_factory=list)
    TABLEATTRIBUTE: List[TableAttribute] = field(default_factory=list)

@dataclass
class TransformField:
    DATATYPE: Optional[str]
    DEFAULTVALUE: Optional[str]
    DESCRIPTION: Optional[str]
    NAME: str
    PICTURETEXT: Optional[str]
    PORTTYPE: Optional[str]
    PRECISION: Optional[str]
    SCALE: Optional[str]

@dataclass
class Transformation:
    DESCRIPTION: Optional[str]
    NAME: str
    OBJECTVERSION: Optional[str]
    REUSABLE: Optional[str]
    TYPE: Optional[str]
    VERSIONNUMBER: Optional[str]
    TRANSFORMFIELD: List[TransformField] = field(default_factory=list)
    TABLEATTRIBUTE: List[TableAttribute] = field(default_factory=list)

@dataclass
class Connector:
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str

@dataclass
class Mapping:
    DESCRIPTION: Optional[str]
    ISVALID: Optional[str]
    NAME: str
    OBJECTVERSION: Optional[str]
    ISPARAM: Optional[str]
    VERSIONNUMBER: Optional[str]
    USERDEFINED: Optional[str]
    TRANSFORMATION: List[Transformation] = field(default_factory=list)
    CONNECTOR: List[Connector] = field(default_factory=list)

@dataclass
class SessionAttribute:
    NAME: str
    VALUE: str

@dataclass
class Session:
    NAME: str
    VALUE: str

@dataclass
class Workflow:
    DESCRIPTION: Optional[str]
    ISENABLED: Optional[str]
    ISRUNNABLESERVICE: Optional[str]
    NAME: str
    OBJECTVERSION: Optional[str]
    OWNERNAME: Optional[str]
    TYPE: Optional[str]
    VARIABLE: Optional[str]
    SESSION: List[Session] = field(default_factory=list)
    ATTRIBUTE: List[SessionAttribute] = field(default_factory=list)

@dataclass
class Task:
    DESCRIPTION: Optional[str]
    NAME: str
    REUSABLE: Optional[str]
    TYPE: str
    VERSIONNUMBER: Optional[str]
    ATTRIBUTE: List[Attribute] = field(default_factory=list)

@dataclass
class Config:
    DESCRIPTION: Optional[str]
    ISDEFAULT: Optional[str]
    NAME: str
    VERSIONNUMBER: Optional[str]
    ATTRIBUTE: List[Attribute] = field(default_factory=list)

@dataclass
class Folder:
    NAME: str
    GROUP: Optional[str]
    OWNER: Optional[str]
    SHARED: Optional[str]
    DESCRIPTION: Optional[str]
    VERSIONNUMBER: Optional[str]
    SOURCE: List[Source] = field(default_factory=list)
    TARGET: List[Target] = field(default_factory=list)
    MAPPING: List[Mapping] = field(default_factory=list)
    TRANSFORMATION: List[Transformation] = field(default_factory=list)
    WORKFLOW: List[Workflow] = field(default_factory=list)
    TASK: List[Task] = field(default_factory=list)
    CONFIG: List[Config] = field(default_factory=list)

@dataclass
class Repository:
    NAME: str
    FOLDER: List[Folder] = field(default_factory=list)

@dataclass
class PowerMart:
    VERSION: Optional[str]
    REPOSITORY: Repository

# Example: instantiate an empty PowerMart object to ensure everything is wired up:
pm = PowerMart(
    VERSION=None,
    REPOSITORY=Repository(
        NAME="ExampleRepo",
        FOLDER=[]
    )
)

# You can now parse your xmltodict output into these classes, for example:
#   repo_dict = data_dict['POWERMART']['REPOSITORY']
#   repo = Repository(NAME=repo_dict['@NAME'], FOLDER=[ ... ])
#   pm = PowerMart(VERSION=data_dict['POWERMART'].get('@VERSION'), REPOSITORY=repo)
