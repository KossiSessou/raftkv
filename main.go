package main // "main" = this is a runnable app, not a library

import "fmt"

func Add(a, b int) int {
	return a + b
}

func main() { // the entry point; Go runs this automatically
	// YOUR LINE: call fmt.Println with a message in double quotes
	fmt.Println(Add(50, 3))
}
