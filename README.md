# Go Partitioning Batch

## Interface
**ParallelDB**

`GetXXX(table name) (반환, error)`


**WriterStorage[K any]**

`GetClient() *K` Writer가 사용할 Storage Client를 반환한다.

**ReaderDB**

`GetReadQuery(table name) query` Reader가 사용할 query를 반환한다.

`ReadDB() *sqlx.DB` Reader가 사용할 sqlx.DB 객체를 반환한다

**DocProcessor[R, J any]**

`ToModel(J) R` Reader에서 반환한 Item을 [J를 이용해] -> [R로 바꿔서] Writer로 전달한다.

**ProcessorParam[J any]**

`GetProcessorParam() (J,error)` Processor.ToModel(J)의 파라미터 J를 결정한다.
