from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from tkinter import *
from tkinter import filedialog as fd


#
# from sqlalchemy import Column, INT, create_engine
# from sqlalchemy.orm import DeclarativeBase, declared_attr, sessionmaker
#
#
# class Base(DeclarativeBase):
#     id = Column(INT, primary_key=True)
#
#     engine = create_engine(url='postgresql://blog:blog@0.0.0.0:5432/blog')
#     session = sessionmaker(bind=engine)
#
#     @declared_attr
#     def __tablename__(cls) -> str:
#         return ''.join(f'_{i.lower()}' if i.isupper() else i for i in cls.__name__).strip('_')
#
#     def update_from_attributes(self, obj) -> None:
#         for k, v in obj.__dict__.items():
#             if hasattr(self, k) and v is not None:
#                 setattr(self, k, v)

### это из fastAPI

#
# from .base import Base
#
#
# class User(Base):
#     __table_args__ = (
#         CheckConstraint('char_length(name) >= 2'),
#     )
#
#     id = Column(CHAR(26), primary_key=True, default=lambda: new().str)
#     name = Column(VARCHAR(length=64), nullable=False)
#     email = Column(VARCHAR(length=128), nullable=False, unique=True)
#     password = Column(VARCHAR(128), nullable=False)
#
#     posts = relationship(argument='Post', back_populates='author')
#
#     def __str__(self) -> str:
#         return f'{self.name}'
#
#     def __repr__(self) -> str:
#         return self.__str__()
#
#     @property
#     def date_of_registration(self) -> datetime:
#         return datetime.fromtimestamp(parse(self.id).timestamp())
#
#
# class Category(Base):
#     __table_args__ = (
#         CheckConstraint('char_length(name) >= 4'),
#         CheckConstraint('char_length(slug) >= 4'),
#     )
#
#     id = Column(SMALLINT, primary_key=True)
#     name = Column(VARCHAR(64), nullable=False, unique=True)
#     slug = Column(VARCHAR(64), nullable=False, unique=True)
#
#     posts = relationship(argument='Post', back_populates='category')
#
#     def __str__(self) -> str:
#         return f'{self.name}'
#
#     def __repr__(self) -> str:
#         return self.__str__()
#
#
# class Post(Base):
#     __table_args__ = (
#         CheckConstraint('char_length(title) >= 4'),
#         CheckConstraint('char_length(slug) >= 4'),
#     )
#
#     title = Column(VARCHAR(128), nullable=False)
#     slug = Column(VARCHAR(164), nullable=False, unique=True)
#     body = Column(TEXT, nullable=False)
#     is_published = Column(BOOLEAN, default=False)
#     date_created = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
#     author_id = Column(CHAR(26), ForeignKey(column='user.id', ondelete='CASCADE'), nullable=False, index=True)
#     category_id = Column(SMALLINT, ForeignKey(column='category.id', ondelete='CASCADE'), nullable=False, index=True)
#
#     category = relationship(argument='Category', back_populates='posts')
#     author = relationship(argument='User', back_populates='posts')
#
#     def __str__(self) -> str:
#         return f'{self.title}'
#
#     def __repr__(self) -> str:
#         return self.__str__()







engine = create_engine("mysql+pymysql://root:***@localhost/newDB", echo=True)

class Store:
    def __init__(self):
        self.t = Text(width=20, height=15, font=("Ariel", 27))
        b1 = Button(text="Загрузить файл покупок", width=15, font=("Ariel", 27))
        b1.bind("<Button-1>", self.buy)
        b2 = Button(text="Загрузить склад", width=15, font=("Ariel", 27))
        b2.bind("<Button-1>", self.loadFile)
        self.t.pack()
        b1.pack()
        b2.pack()

    def loadFile(self, event):
        with open("base", "r") as f:
            products_str = f.readlines()
        with Session(autoflush=False, bind=engine) as db:
            db.query(Food).delete()
            for i in products_str:
                x = i.split()
                db.add(Food(type=x[0], manufacturer=x[1], price=x[2], quantity=x[3]))
            db.commit()
        self.showText()

    def showText(self):
        self.t.delete(1.0, END)
        with Session(autoflush=False, bind=engine) as db:
            db_products = db.query(Food)
        for i in db_products:
            self.t.insert(END, str(i) + "\n")

    def buy(self, event):
        with open(fd.askopenfilename(), "r") as f:
            buy_str = f.readlines()
        with Session(autoflush=False, bind=engine) as db:
            for i in buy_str:
                x = i.split()
                item = db.query(Food).filter(Food.type == x[0]).first()
                item.quantity -= int(x[3])
            db.commit()
        self.showText()


class Base(DeclarativeBase):
    pass


class Food(Base):
    __tablename__ = "food"
    id = Column(Integer, primary_key=True, index=True)
    type = Column(String(50))
    manufacturer = Column(String(100))
    price = Column(Integer)
    quantity = Column(Integer)
    def __str__(self):
        return f"{self.type} {self.manufacturer} {self.price} {self.quantity}"


Base.metadata.create_all(bind=engine)

root = Tk()
root.title("Магазин")
w, h = (root.winfo_screenwidth() // 2), (root.winfo_screenheight() // 2)
size_x, size_y = 450, 600
root.geometry(f"{size_x}x{size_y}+{w - size_x // 2}+{h - size_y // 2}")

store = Store()

root.mainloop()
