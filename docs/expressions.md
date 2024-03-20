# Expressions Query Language (QL)
Solaris allows you to specify filters for selecting logs and their records in the form of Boolean expressions. An expression is a text that contains constants, arguments, and operations that form the query for selecting logs and their records. We will try to explain it briefly here so that the reader can apply it immediately.

QL is a very simple language for writing boolean expressions. For example:

to select logs with tag "abc"="def" or for the log with ID="1234123421342343":
```
tag('abc') = tag("def") OR logID = "1234123421342343" 
```

to select records created between February 12, and March 11 12:34:43 of 2024:
```
ctime < "2024-03-11 12:34:43.000" AND ctime > "2024-02-12 00:00:00.000"   
```

## Arguments
An argument is a value, which can be referenced by one of the following forms:
- constant
- identifier
- function
- list of constants

### Constant values
QL supports one type of constants - string. The string constant is a text in double or single quotes. The numbers are either natural or real numbers.

String constants examples:
```
''
""
"Hello world"
'Andrew said: "Hello!"'
```

### Identifiers
Identfier is a variable, which adressed by name. QL supports the following identifiers:
- `logID` - the log unique identifier.
- `ctime` - the record created time (every record gets its ctime when it is added to the log). For `ctime` only the `<` and `>` operations are allowed.

### Functions
A function is a value that is calculated from the arguments provided. It looks like an identifier followed by arguments in parentheses. The argument list may be empty.

Solaris supports the following functions:
- `tag(<name>)` - returns the tag value for a log. Name could be a string constant or any other argument value

### List of constants
Some constants maybe groupped in a list. The List defined like the coma-separted constants in between `[` and `]`:

```
["a", "b", 'c'] 
```
the list of three string constants - "a", "b" and "c"

## Operations
Operation is an action which requires two arguments. All operations return either TRUE or FALSE

Examples:
```
'1234' = '234' // compares two string constants, the result will be FALSE
logID != "123" // compares log ID with the string "123", the result depends on the logID value
tag("t1") > tag("t2") // compares value of the tag t1 with the value of the tag t2, the result depends on the tags values
tag("t1") IN ["1", "2", "3"] // the value of t1 is either "1", "2", or "3"
tag("t1") LIKE 'abc%' // matches the value of tag t1 against the pattern 'abc%', where '%' is a wildcard that matches any sequence of characters  
```

QL supports the following operations:

"<", ">", "<=", ">=", "!=", "="

| Operation | Description                                                                                                 |
|-----------|-------------------------------------------------------------------------------------------------------------|
| <         | The left argument is less than the right one                                                                |
| >         | The left argument is greater than the right one                                                             |
| <=        | The left argument is less or equal to the right one                                                         |
| >=        | The left argument is greater or equal to the right one                                                      |
| !=        | The left argument is not equal to the right one                                                             |
| =         | The left argument is equal to the right one                                                                 |
| IN        | The left argument value is in the list. Right argument must be a list                                       |
| LIKE      | The left argument should be like the constant (second argument). The operation is similart to the SQL like. |

## QL boolen expression
The QL expression is the series of boolean values that can be combined by AND, OR, NOT boolean operations and the parenthesis to increase the priority.

Examples:
```
tag('t1') != tag('t2') OR tag('t1') = 'abc' 
ctime > "2024-02-12 00:00:00.000" AND ctime < "2024-03-12 00:00:00.000"
```

## That is it
With all the information above you can define a filter in a form of QL boolean expression.