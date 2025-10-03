package mr

const (
	IDLE    int = 0
	WORKING int = 1
)

// 任务状态
const (
	START    int = 0
	DOING    int = 1
	WRITTING int = 2 //写入状态
	DONE     int = 3
)

const (
	TASKTYPEMAP    int = 0
	TASKTYPEREDUCE int = 1
)

const (
	MAXGETTASKFAILEDCOUNT = 100
)
