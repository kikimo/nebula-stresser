# nebula-stresser

## getting started

```shell
$ go build
$ ./nebula-stresser -h
A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.

Usage:
  nebula-stresser [command]

Available Commands:
  checkedge   Check integrity of nebula edge
  completion  generate the autocompletion script for the specified shell
  help        Help about any command

Flags:
      --config string   config file (default is $HOME/.nebula-stresser.yaml)
  -h, --help            help for nebula-stresser
  -t, --toggle          Help message for toggle

Use "nebula-stresser [command] --help" for more information about a command.

$ ./nebula-stresser checkedge | more
total edges found: 12772
found 12772 forward edges
total edges found: 12772
found 12772 backward edges
found 12772 edges in total
found 12772 semi-normal edges()
prop mismatch in edge: 12->76, forward prop: 15-1484, backward: 33-1484
found 3612 missing edges:
1->3
1->6
1->9
1->12
1->15
1->18
1->21
1->24
...
```

