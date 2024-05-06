import asyncio
import uvicorn

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, Integer, String, select, update 
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker  

from contextlib import asynccontextmanager 

Base = declarative_base()  
class Book(Base):
    __tablename__ = "books"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    author = Column(String)
    genre = Column(String)  

class BookRepository:
    def __init__(self):  
        self.semaphore = asyncio.Semaphore(50)  
        self.engine = create_async_engine('sqlite+aiosqlite:///books.db', echo=False)
        self.session = async_sessionmaker(self.engine, expire_on_commit=False)

    async def create_table(self): 
        async with self.engine.begin() as engine:  
            await engine.run_sync(Base.metadata.create_all)    
     
    async def create_book(self, title: str, author: str, genre: str):
        async with self.semaphore:
            try:
                book = Book(title=title, author=author, genre=genre)
                async with self.session.begin() as session:
                    session.add(book)
                return True
            except:
                async with self.session.begin() as session:
                    await session.rollback()
                return False
            
    async def get_books(self):
        async with self.semaphore:
            async with self.session.begin() as session:
                query = select(Book)
                result = await session.stream(query)
                books = await result.scalars().all()
            
            return books

    async def get_book_by_id(self, id: int):
        async with self.semaphore:
            async with self.session.begin() as session:
                query = select(Book).where(Book.id == id)
                result = await session.stream(query)
                book = await result.scalars().one_or_none()
            
            return book
    
    async def update_book(self, id: int, **fields): 
        async with self.semaphore:
            try:
                async with self.session.begin() as session:
                    query = update(Book).where(Book.id == id).values(fields)
                    await session.execute(query)
                return True
            except:
                async with self.session.begin() as session:
                    await session.rollback()
                return False

    async def delete_book(self, book: Book):
        async with self.semaphore:
            async with self.session.begin() as session:
                await session.delete(book)

            return True
    
    async def close(self):
        await self.engine.dispose() 

class Api:
    def __init__(self, app: FastAPI) -> None:
        self.repo = BookRepository()
        self.app = app    

    def api(self):
        @self.app.get("/book/{id}", status_code=status.HTTP_200_OK, tags=["Book"])
        async def get_book(id: int):
            book = await self.repo.get_book_by_id(id)
            return book

        @self.app.get("/books", status_code=status.HTTP_200_OK, tags=["Book"])
        async def get_books():
            books = await self.repo.get_books()
            return books
            
        @self.app.post("/book", status_code=status.HTTP_201_CREATED, tags=["Book"])
        async def create_book(title: str, author: str, genre: str):
            ret = await self.repo.create_book(title=title, author=author, genre=genre)
            return ret

        @self.app.put("/book/{id}", status_code=status.HTTP_200_OK, tags=["Book"])
        async def update_book(id: int, title: str = None, author: str = None, genre: str = None):
            fields = {
                "title": title,
                "author": author,
                "genre": genre
            }
            ret = await self.repo.update_book(id, **fields)
            return ret

        @self.app.delete("/book/{id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Book"])
        async def delete_book(id: int):
            book = await self.repo.get_book_by_id(id)
            await self.repo.delete_book(book)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await BookRepository().create_table()
    yield
    await BookRepository().close()

app = FastAPI(
    title="Books API",
    description="A simple api for CRUD operations",
    version="1.0",
    swagger_ui_parameters={ "defaultModelsExpandDepth": -1 },
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

Api(app).api()

if __name__ == "__main__": 
    uvicorn.run(app, host="0.0.0.0", port=8000)