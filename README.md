# Sublevel for Go

Sublevel offers multiple sections within on LevelDB.
Furthermore, there are hooks that allow you to run custom code whenever a row is put or deleted in a section.

This library is a Go implementation of the ideas found in the level-sublevel project of Dominic Tarr, which is written in JavaScript.

Sublevel uses levigo as a Go wrapper around LevelDB.
The API of a section is similar to the API offered by levigo for the entire database.
