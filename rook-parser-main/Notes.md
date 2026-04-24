# Query Processor

* To implement Query processor with initial goals of implementing **Query Compiler** that contains **Query Parser:** Syntactic Parsing and Semantic Parsing.

### Query Processor
* In Between User Applications and Storage Manager of RDBMS.

---
![Processor Architecture](assets/Processor-Architecture.png)

---
![Query Processor Types](assets/Processor-Types.jpeg)

---
<table>
  <tr>
    <td>
      <img src="assets/query-processor-components.jpeg" width="400">
    </td>
    <td>
      <img src="assets/outline-query-compiler.jpeg" width="400">
      <p><em>Query Parser output is Query Expression Tree.</em></p>
    </td>
  </tr>
</table>

---
![Query Compiler Steps](assets/Compiler-Steps.png)

* Query Compiler:
    - Lexical Analysis
    - Syntactic Analysis
    - Semantic Analysis
    - Query Optimization

---
![Query Plan Steps](assets/Query-Plan-Steps.png)

---
![DDL and DML](assets/DDL-Interpreter-DML.png)

* DDL Interpreter doesn't require Query Optimisation
---


## Parser
* The job of the parser is to take text written in a language such as SQL and convert into a **Parse Tree.**


### Parse Tree
* Tree whose nodes correspond to either:
    - Atoms: lexical elements such as keywords, names of attributes or relations, constants, parentheses, operators such as + or <, and other schema elements
    - Syntactic Categories: names for families of query subparts that all play a similar role in the query.


---
![Query Logical Plan](assets/Parser-Logical-Plan.jpeg)

---
![Parser](assets/Parser.png)

![Query outline](assets/queryoutline.png)
![Parsing Example](assets/parsing_example.png)
![query processing steps](assets/query_processing_steps.png)