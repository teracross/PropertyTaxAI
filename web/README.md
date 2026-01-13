Web app for retrieving, viewing and downloading query results from PostGres SQL database

Future Features (In Progress): 

[&#x2714;] allow downloading of query results in multiple formats (CSV, JSON, etc.)

[&#x2714;] textbox to handle natural language queries as well as custom SQL queries

[&#x2714;] unique-ID for caching context window? (assigning unique ID to context sent to server for faster and more repeatable results, maybe consider using REDIS or another db for user session information to allow for retries/ longer queries) 
* remove Django cookies for session data storage 

[ ] add pagination for results returned on web page

[ ] determine how to package and return results in zipped file (some results might be quite large)
