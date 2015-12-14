# TinyMSQL
Interpreter accepts SQL queries that are valid in terms of the grammar of Tiny-SQL (see TinySQL_Grammar.pdf), execute the queries, and output the results of the execution. Interpreter includes the following components:
##A parser: 
The parser accepts the input Tiny-SQL query and converts it into a parse tree. 
##A logical Optimizer:
The logical optimizer includes logical query plan optimization. In particular, it implements the ideas of pushing selections down, it breaks down the search condition into multiple search conditions for each table and pair of tables to be used for selection optmization.
##A physical query plan generator: 
This generator converts a parsed tree into an executable physical query plan. This phase also includes physical query plan
optimizations like placing a smaller table on the left for binary operations. Finding the best order for join and cross product.

*NOTE: Please read following files for better understanding of the project.
  * **How_to_run.pdf**: How to run the project.
  * **Project_Problem.pdf**: For understanding the problem solved and learn about the Storage Manager library used in the project to   simulate a disk.
  * **TinySQL_Grammar.pdf**: The Grammar implemened in the parser and to see the type of queries accepted.
  * **Final_Report.pdf**: In depth understanding of the project workflow and various optimizations done on logical and physical level.
