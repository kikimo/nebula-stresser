module github.com/kikimo/nebula-stresser

go 1.16

require (
	github.com/anishathalye/porcupine v0.1.2
	github.com/facebook/fbthrift v0.31.1-0.20210223140454-614a73a42488
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/spf13/cobra v1.2.1
	github.com/spf13/viper v1.8.1
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/vesoft-inc/nebula-go/v2 v2.5.1
)

// replace github.com/vesoft-inc/nebula-go/v2 v2.5.1 => github.com/vesoft-inc/nebula-go v1
replace github.com/vesoft-inc/nebula-go/v2 v2.5.1 => github.com/kikimo/nebula-go/v2 v2.6.1 // indirect
