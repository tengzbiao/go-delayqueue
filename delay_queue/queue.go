package delay_queue

type Queue interface {
	AddJob(job *Job) error
	RemoveJob(jobId string) error
	GetJob(jobId string) (*Job, error)
	Schedule(stop <-chan struct{}) error
	Close()
}
