# SQLChain

<img src="https://img.shields.io/badge/Version-1.0.0-blue.svg" alt="Version Badge">
<img src="https://img.shields.io/badge/License-Apache%202.0-green.svg" alt="License Badge">

SQLChain is a lightweight and efficient chain-style data processing library for SQL databases. It enables you to process data from SQL queries using fluent, chainable operations with built-in support for parallel processing.

## Features

- **Fluent API**: Chain operations together in a readable and expressive syntax
- **Parallel Processing**: Built-in support for parallel execution of operations
- **SQL Integration**: Direct integration with SQL databases via SQLAlchemy
- **Type Safety**: Generic type support for better IDE assistance and safer code
- **Async Support**: Async operations for non-blocking execution of parallel workloads
- **Error Handling**: Comprehensive error handling with custom exceptions

## Requirements

- Python 3.12+
- Required packages:
  - sqlalchemy
  - pymysql
  - httpx
  - python-dotenv
  - beautifulsoup4
  - openai

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/sqlchain.git
   cd sqlchain
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install openai httpx python-dotenv sqlalchemy pymysql beautifulsoup4
   ```

## Configuration

Create a `.env` file in your project root with the following variables:

```
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=3306
DB_DATABASE=your_database
OPENAI_API_KEY=your_openai_key
OLLAMA_URL=your_ollama_url  # Optional, if using Ollama
```

## Usage Examples

### Basic SQL Query and Processing

```python
from sqlchain import SQLChain
import sqlalchemy as sa

# Create database engine
engine = sa.create_engine("mysql+pymysql://user:password@localhost/dbname")
chain = SQLChain(engine)

# Execute SQL query and collect results
results = (chain.sql("SELECT * FROM users WHERE active = 1")
           .map(lambda user: user['name'].upper())
           .filter(lambda name: len(name) > 5)
           .collect())

print(results)
```

### Parallel Processing with Async

```python
import asyncio

async def process_data():
    # Process data in parallel with 10 workers
    total_length = await (chain.sql("SELECT * FROM articles")
                         .parallel(num_workers=10, chunk_size=20)
                         .map(lambda article: len(article['content']))
                         .reduce(lambda x, y: x + y, initial=0))
    
    print(f"Total content length: {total_length}")

# Run the async function
asyncio.run(process_data())
```

### Group By Operation

```python
# Group users by country and collect results
country_groups = (chain.sql("SELECT * FROM users")
                  .group_by(lambda user: user['country'])
                  .collect())

for country, users in country_groups:
    print(f"{country}: {len(users)} users")
```

## API Documentation

### SQLChain

The main entry point for SQL-based operations.

```python
chain = SQLChain(engine)
stream = chain.sql("SELECT * FROM table", params={"param": value})
```

**Methods**:
- `sql(sql_query, params=None)`: Executes SQL query and returns a Stream of results

### Stream

Sequential stream processor for data operations.

**Methods**:
- `map(func)`: Apply function to each element
- `filter(predicate)`: Filter elements by predicate
- `group_by(key_func)`: Group elements by key function
- `parallel(num_workers, chunk_size, timeout)`: Convert to parallel stream
- `collect()`: Collect all elements into a list
- `stats()`: Get execution statistics

### ParallelStream

Parallel stream processor for concurrent data operations.

**Methods**:
- `map(func)`: Apply function to each element in parallel
- `filter(predicate)`: Filter elements by predicate in parallel
- `reduce(func, initial=None)`: Reduce elements using function in parallel
- `collect()`: Collect all elements into a list asynchronously

## Error Handling

SQLChain provides custom exceptions for better error handling:

```python
from sqlchain import StreamError, SQLExecutionError, ParallelExecutionError

try:
    results = chain.sql("SELECT * FROM nonexistent_table").collect()
except SQLExecutionError as e:
    print(f"SQL error: {e}")
except StreamError as e:
    print(f"Stream processing error: {e}")
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.