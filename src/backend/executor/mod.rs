pub mod duplicate;
pub mod expr;
pub mod load_csv;
pub mod projection;
pub mod projection_bench;
pub mod projection_enhanced;
pub mod projection_optimized;
pub mod projection_benchmark_suite;
pub mod seq_scan;
pub mod set_ops;
pub mod streaming;
pub mod tuple_codec;
pub mod value;

pub use duplicate::{
    build_duplicate_index, copy_deduped, copy_duplicates_only,
    load_duplicate_index, DuplicateReport, TupleLocation,
};
pub use load_csv::load_csv;
pub use projection::{
    apply_distinct, eval_projection_list, filter_rows, load_rows,
    project, select, OutputColumn, ProjectionInput, ProjectionItem, ResultTable,
};
pub use projection_enhanced::{
    ProjectionEngine, ProjectionResult, ProjectionStatus, ProjectionMetrics,
    ColumnReorderSpec, FilterConfig, save_projection_to_temp,
};
pub use seq_scan::show_tuples;
pub use set_ops::{except, intersect, union};
pub use streaming::{stream_count, stream_dedup_scan, stream_project, stream_select, StreamResult};
pub use projection_optimized::{
    ReorderStrategy, OptimizedReorderConfig, ReorderMetrics, 
    reorder_optimized, predict_best_strategy,
};
pub use projection_benchmark_suite::{
    BenchmarkConfig, StrategyBenchmark, BenchmarkResult, StrategyComparison,
};
