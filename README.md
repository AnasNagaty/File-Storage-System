# File-Storage-System

A distributed file storage system that has the following capabilities:
1. Files need to be distributed across multiple nodes and load shall balance automatically with addition of new nodes.
2. Make sure data is not lost if a node fails by setting up mechanisms for data replication across several nodes and automatic failover.
3. Conserver storage by creating a method to identify and eliminate redundant instances of duplicate data.
4. Create suitable indexing to facilitate quick searches.
5. Use authentication and authorization to access the stored data.
6. Monitor performance and record events for the purpose of auditing and resolving issues.
7. Develop a series of RESTful APIs for the system to include a new node, saving, fetching, and removing a file.
8. Create tests for the RESTful APIs to ensure their correct functionality.
