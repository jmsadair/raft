package goraft

type Command interface{}

type LogEntry struct {
	term    uint64
	index   uint64
	command Command
}

func NewLogEntry(term uint64, index uint64, command Command) *LogEntry {
	return &LogEntry{term: term, index: index, command: command}
}

func (entry *LogEntry) Term() uint64 {
	return entry.term
}

func (entry *LogEntry) SetTerm(term uint64) {
	entry.term = term
}

func (entry *LogEntry) Index() uint64 {
	return entry.index
}

func (entry *LogEntry) SetIndex(index uint64) {
	entry.index = index
}

func (entry *LogEntry) Command() Command {
	return entry.command
}

func (entry *LogEntry) SetCommand(command Command) {
	entry.command = command
}
