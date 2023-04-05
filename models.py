from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base


class BaseModel:
    __abstract__ = True

    def keyvalgen(self):
        """ Generate attr name/val pairs, filtering out SQLA attrs."""
        excl = ('_sa_adapter', '_sa_instance_state')
        for k, v in vars(self).items():
            if not k.startswith('_') and not any(hasattr(v, a) for a in excl):
                yield k, v

    def __repr__(self):
        params = ', '.join(f'{k}={v}' for k, v in self.keyvalgen())
        return f"{self.__class__.__name__}({params})"

    def __str__(self):
        return self.__repr__()


Base = declarative_base(cls=BaseModel)


class Brewery(Base):
    __tablename__ = "breweries"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    slug = Column(String, unique=True)
    beers = relationship("Beer", back_populates="brewery")


class Beer(Base):
    __tablename__ = "beers"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    alc_percentage = Column(Float)
    brewery_id = Column(Integer, ForeignKey("breweries.id"))
    brewery = relationship("Brewery", back_populates="beers")
    reviews = relationship("Review", back_populates="beer")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    slug = Column(String, unique=True)
    description = Column(String)
    reviews = relationship("Review", back_populates="user")


class Review(Base):
    __tablename__ = "reviews"
    id = Column(Integer, primary_key=True)
    percentage = Column(Float)
    comment = Column(String)
    date = Column(DateTime)
    beer_id = Column(Integer, ForeignKey("beers.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    beer = relationship("Beer", back_populates="reviews")
    user = relationship("User", back_populates="reviews")
