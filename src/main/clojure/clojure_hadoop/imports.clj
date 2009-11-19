(ns clojure-hadoop.imports
;;#^{:doc "Functions to import entire packages under org.apache.hadoop."}
)

(defn import-io
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.io into the current namespace."
  []
  (import '(org.apache.hadoop.io RawComparator
SequenceFile$Sorter$RawKeyValueIterator SequenceFile$ValueBytes
Stringifier Writable WritableComparable WritableFactory
AbstractMapWritable ArrayFile ArrayFile$Reader ArrayFile$Writer
ArrayWritable BooleanWritable BooleanWritable$Comparator BytesWritable
BytesWritable$Comparator ByteWritable ByteWritable$Comparator
CompressedWritable DataInputBuffer DataOutputBuffer DefaultStringifier
DoubleWritable DoubleWritable$Comparator FloatWritable
FloatWritable$Comparator GenericWritable InputBuffer LongWritable
LongWritable$Comparator IOUtils IOUtils$NullOutputStream LongWritable
LongWritable$Comparator LongWritable$DecreasingComparator MapFile
MapFile$Reader MapFile$Writer MapWritable MD5Hash MD5Hash$Comparator
NullWritable NullWritable$Comparator ObjectWritable OutputBuffer
SequenceFile SequenceFile$Metadata SequenceFile$Reader
SequenceFile$Sorter SequenceFile$Writer SetFile SetFile$Reader
SetFile$Writer SortedMapWritable Text Text$Comparator
TwoDArrayWritable UTF8 UTF8$Comparator VersionedWritable VLongWritable
VLongWritable WritableComparator WritableFactories WritableName
WritableUtils SequenceFile$CompressionType MultipleIOException
VersionMismatchException)))

(defn import-io-compress 
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.io.compress into the current namespace."
  []
  (import '(org.apache.hadoop.io.compress CompressionCodec Compressor
Decompressor BlockCompressorStream BlockDecompressorStream CodecPool
CompressionCodecFactory CompressionInputStream CompressionOutputStream
CompressorStream DecompressorStream DefaultCodec GzipCodec
GzipCodec$GzipInputStream GzipCodec$GzipOutputStream)))

(defn import-fs 
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.fs into the current namespace."
  []
  (import '(org.apache.hadoop.fs PathFilter PositionedReadable
Seekable Syncable BlockLocation BufferedFSInputStream
ChecksumFileSystem ContentSummary DF DU FileStatus FileSystem
FileSystem$Statistics FileUtil FileUtil$HardLink FilterFileSystem
FSDataInputStream FSDataOutputStream FSInputChecker FSInputStream
FSOutputSummer FsShell FsUrlStreamHandlerFactory HarFileSystem
InMemoryFileSystem LocalDirAllocator LocalFileSystem Path
RawLocalFileSystem Trash ChecksumException FSError)))

(defn import-mapred 
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.mapred into the current namespace."
  []
  (import '(org.apache.hadoop.mapred InputFormat InputSplit
JobConfigurable JobHistory$Listener Mapper MapRunnable OutputCollector
OutputFormat Partitioner RawKeyValueIterator RecordReader RecordWriter
Reducer Reporter RunningJob SequenceFileInputFilter$Filter
ClusterStatus Counters Counters$Counter Counters$Group
DefaultJobHistoryParser FileInputFormat FileOutputFormat FileSplit ID
IsolationRunner JobClient JobConf JobEndNotifier JobHistory
JobHistory$HistoryCleaner JobHistory$JobInfo JobHistory$MapAttempt
JobHistory$ReduceAttempt JobHistory$Task JobHistory$TaskAttempt JobID
JobProfile JobStatus JobTracker KeyValueLineRecordReader
KeyValueTextInputFormat LineRecordReader LineRecordReader$LineReader
MapFileOutputFormat MapReduceBase MapRunner MultiFileInputFormat
MultiFileSplit OutputLogFilter SequenceFileAsBinaryInputFormat
SequenceFileAsBinaryInputFormat$SequenceFileAsBinaryRecordReader
SequenceFileAsBinaryOutputFormat
SequenceFileAsBinaryOutputFormat$WritableValueBytes
SequenceFileAsTextInputFormat SequenceFileAsTextRecordReader
SequenceFileInputFilter SequenceFileInputFilter$FilterBase
SequenceFileInputFilter$MD5Filter
SequenceFileInputFilter$PercentFilter
SequenceFileInputFilter$RegexFilter SequenceFileInputFormat
SequenceFileOutputFormat SequenceFileRecordReader TaskAttemptID
TaskCompletionEvent TaskID TaskLog TaskLogAppender TaskLogServlet
TaskReport TaskTracker TaskTracker$MapOutputServlet TextInputFormat
TextOutputFormat TextOutputFormat$LineRecordWriter
JobClient$TaskStatusFilter JobHistory$Keys JobHistory$RecordTypes
JobHistory$Values JobPriority JobTracker$State
TaskCompletionEvent$Status TaskLog$LogName FileAlreadyExistsException
InvalidFileTypeException InvalidInputException InvalidJobConfException
JobTracker$IllegalStateException)))

(defn import-mapred-lib 
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.mapred.lib into the current namespace."
  []
  (import '(org.apache.hadoop.mapred.lib FieldSelectionMapReduce
HashPartitioner IdentityMapper IdentityReducer InverseMapper
KeyFieldBasedPartitioner LongSumReducer MultipleOutputFormat
MultipleSequenceFileOutputFormat MultipleTextOutputFormat
MultithreadedMapRunner NLineInputFormat NullOutputFormat RegexMapper
TokenCountMapper)))


