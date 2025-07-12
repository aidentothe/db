import re
import sqlparse
from sqlparse.sql import Statement, IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, Name
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import logging
from datetime import datetime

class QueryComplexityAnalyzer:
    """Analyzes SQL queries to determine memory and compute complexity"""
    
    def __init__(self):
        self.operation_weights = {
            'SELECT': 1,
            'FROM': 1,
            'WHERE': 2,
            'GROUP BY': 3,
            'HAVING': 3,
            'ORDER BY': 2,
            'JOIN': 4,
            'INNER JOIN': 4,
            'LEFT JOIN': 5,
            'RIGHT JOIN': 5,
            'FULL JOIN': 6,
            'UNION': 3,
            'UNION ALL': 2,
            'DISTINCT': 2,
            'SUBQUERY': 4,
            'WINDOW FUNCTION': 5,
            'CASE': 2,
            'AGGREGATE': 3
        }
        
        self.aggregate_functions = {
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 
            'GROUP_CONCAT', 'STRING_AGG', 'ARRAY_AGG'
        }
        
        self.window_functions = {
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
            'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE'
        }

    def analyze_query(self, query: str, data_stats: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Main method to analyze a SQL query and return complexity metrics
        
        Args:
            query: SQL query string
            data_stats: Dictionary containing data statistics (row count, column info, etc.)
            
        Returns:
            Dictionary containing complexity analysis results
        """
        try:
            # Parse the SQL query
            parsed = sqlparse.parse(query)[0]
            
            # Extract query components
            components = self._extract_query_components(parsed)
            
            # Calculate complexity scores
            compute_score = self._calculate_compute_complexity(components)
            memory_score = self._calculate_memory_complexity(components, data_stats)
            
            # Estimate performance metrics
            performance_estimate = self._estimate_performance(components, data_stats)
            
            # Generate optimization suggestions
            suggestions = self._generate_suggestions(components, compute_score, memory_score)
            
            # Calculate overall complexity rating (1-10)
            overall_complexity = min(10, max(1, round((compute_score + memory_score) / 2)))
            
            return {
                'query': query,
                'complexity_rating': overall_complexity,
                'compute_score': compute_score,
                'memory_score': memory_score,
                'components': components,
                'performance_estimate': performance_estimate,
                'optimization_suggestions': suggestions,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"Error analyzing query: {str(e)}")
            return {
                'query': query,
                'error': str(e),
                'complexity_rating': 1,
                'compute_score': 1,
                'memory_score': 1
            }

    def _extract_query_components(self, parsed_query: Statement) -> Dict[str, Any]:
        """Extract and categorize components from parsed SQL query"""
        components = {
            'tables': [],
            'columns': [],
            'joins': [],
            'where_conditions': [],
            'group_by': [],
            'order_by': [],
            'having': [],
            'aggregate_functions': [],
            'window_functions': [],
            'subqueries': 0,
            'unions': 0,
            'distinct': False,
            'case_statements': 0,
            'functions': []
        }
        
        def extract_from_token(token):
            if token.ttype is Keyword:
                keyword = token.value.upper()
                if 'JOIN' in keyword:
                    components['joins'].append(keyword)
                elif keyword == 'DISTINCT':
                    components['distinct'] = True
                elif keyword == 'UNION':
                    components['unions'] += 1
                    
            elif token.ttype in Name or token.ttype is None:
                if isinstance(token, Function):
                    func_name = str(token).split('(')[0].upper()
                    components['functions'].append(func_name)
                    if func_name in self.aggregate_functions:
                        components['aggregate_functions'].append(func_name)
                    elif func_name in self.window_functions:
                        components['window_functions'].append(func_name)
                        
            if hasattr(token, 'tokens'):
                for subtoken in token.tokens:
                    extract_from_token(subtoken)
                    
            # Count subqueries
            if 'SELECT' in str(token).upper() and token != parsed_query:
                components['subqueries'] += 1
                
            # Count CASE statements
            if 'CASE' in str(token).upper():
                components['case_statements'] += str(token).upper().count('CASE')
        
        for token in parsed_query.tokens:
            extract_from_token(token)
            
        # Extract table names
        tables = self._extract_table_names(str(parsed_query))
        components['tables'] = tables
        
        return components

    def _extract_table_names(self, query: str) -> List[str]:
        """Extract table names from SQL query using regex"""
        # Remove comments and normalize whitespace
        query = re.sub(r'--.*?\n', ' ', query)
        query = re.sub(r'/\*.*?\*/', ' ', query, flags=re.DOTALL)
        query = re.sub(r'\s+', ' ', query)
        
        # Find table names after FROM and JOIN keywords
        table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        tables = re.findall(table_pattern, query, re.IGNORECASE)
        
        return list(set(tables))

    def _calculate_compute_complexity(self, components: Dict[str, Any]) -> float:
        """Calculate compute complexity score based on operations"""
        score = 0
        
        # Base operations
        score += len(components['tables'])
        score += len(components['joins']) * self.operation_weights['JOIN']
        score += len(components['aggregate_functions']) * self.operation_weights['AGGREGATE']
        score += len(components['window_functions']) * self.operation_weights['WINDOW FUNCTION']
        score += components['subqueries'] * self.operation_weights['SUBQUERY']
        score += components['unions'] * self.operation_weights['UNION']
        score += components['case_statements'] * self.operation_weights['CASE']
        
        if components['distinct']:
            score += self.operation_weights['DISTINCT']
            
        if components['group_by']:
            score += self.operation_weights['GROUP BY']
            
        if components['order_by']:
            score += self.operation_weights['ORDER BY']
            
        if components['having']:
            score += self.operation_weights['HAVING']
        
        # Normalize to 1-10 scale
        return min(10, max(1, score / 2))

    def _calculate_memory_complexity(self, components: Dict[str, Any], data_stats: Dict[str, Any] = None) -> float:
        """Calculate memory complexity based on data size and operations"""
        if not data_stats:
            # Default scoring without data stats
            base_score = 2
            if len(components['joins']) > 0:
                base_score += len(components['joins']) * 1.5
            if components['subqueries'] > 0:
                base_score += components['subqueries'] * 2
            if len(components['aggregate_functions']) > 0:
                base_score += len(components['aggregate_functions']) * 1.2
            return min(10, max(1, base_score))
        
        # Calculate based on actual data statistics
        row_count = data_stats.get('row_count', 1000)
        column_count = data_stats.get('column_count', 10)
        
        # Base memory requirement
        base_memory_mb = (row_count * column_count * 8) / (1024 * 1024)  # Rough estimate
        
        # Apply multipliers based on operations
        memory_multiplier = 1.0
        
        # Joins increase memory requirements significantly
        if components['joins']:
            memory_multiplier += len(components['joins']) * 0.5
            
        # Group by operations require intermediate storage
        if components['group_by']:
            memory_multiplier += 0.3
            
        # Subqueries require additional temporary storage
        if components['subqueries']:
            memory_multiplier += components['subqueries'] * 0.4
            
        # Window functions require partitioning data in memory
        if components['window_functions']:
            memory_multiplier += len(components['window_functions']) * 0.3
            
        # Distinct operations require deduplication
        if components['distinct']:
            memory_multiplier += 0.2
            
        estimated_memory_mb = base_memory_mb * memory_multiplier
        
        # Convert to 1-10 scale (log scale for large datasets)
        if estimated_memory_mb < 10:
            return 1
        elif estimated_memory_mb < 100:
            return 3
        elif estimated_memory_mb < 500:
            return 5
        elif estimated_memory_mb < 1000:
            return 7
        elif estimated_memory_mb < 5000:
            return 9
        else:
            return 10

    def _estimate_performance(self, components: Dict[str, Any], data_stats: Dict[str, Any] = None) -> Dict[str, Any]:
        """Estimate query performance metrics"""
        if not data_stats:
            row_count = 10000  # Default assumption
        else:
            row_count = data_stats.get('row_count', 10000)
        
        # Base execution time estimate (in seconds)
        base_time = row_count / 50000  # Assume 50K rows per second baseline
        
        # Apply complexity multipliers
        time_multiplier = 1.0
        
        # Each join roughly doubles processing time
        if components['joins']:
            time_multiplier *= (2 ** len(components['joins']))
            
        # Subqueries add significant overhead
        if components['subqueries']:
            time_multiplier *= (1.5 ** components['subqueries'])
            
        # Aggregations slow down processing
        if components['aggregate_functions']:
            time_multiplier *= (1.3 ** len(components['aggregate_functions']))
            
        # Window functions are expensive
        if components['window_functions']:
            time_multiplier *= (1.8 ** len(components['window_functions']))
            
        estimated_time = base_time * time_multiplier
        
        # Estimate result set size
        result_size_multiplier = 1.0
        if components['group_by']:
            result_size_multiplier *= 0.1  # Grouping typically reduces result size
        if components['joins']:
            result_size_multiplier *= (1.5 ** len(components['joins']))  # Joins can increase size
        if components['distinct']:
            result_size_multiplier *= 0.8  # Distinct reduces duplicates
            
        estimated_result_rows = int(row_count * result_size_multiplier)
        
        return {
            'estimated_execution_time_seconds': round(estimated_time, 2),
            'estimated_result_rows': estimated_result_rows,
            'memory_usage_category': self._categorize_memory_usage(estimated_time, row_count),
            'performance_category': self._categorize_performance(estimated_time)
        }

    def _categorize_memory_usage(self, execution_time: float, row_count: int) -> str:
        """Categorize memory usage level"""
        memory_score = (execution_time * row_count) / 100000
        
        if memory_score < 1:
            return "Low"
        elif memory_score < 5:
            return "Medium"
        elif memory_score < 20:
            return "High"
        else:
            return "Very High"

    def _categorize_performance(self, execution_time: float) -> str:
        """Categorize expected performance"""
        if execution_time < 1:
            return "Fast"
        elif execution_time < 10:
            return "Medium"
        elif execution_time < 60:
            return "Slow"
        else:
            return "Very Slow"

    def _generate_suggestions(self, components: Dict[str, Any], compute_score: float, memory_score: float) -> List[str]:
        """Generate optimization suggestions based on query analysis"""
        suggestions = []
        
        # General suggestions based on complexity scores
        if compute_score > 7:
            suggestions.append("Consider simplifying the query by breaking it into smaller parts")
            
        if memory_score > 7:
            suggestions.append("Query may require significant memory - consider adding LIMIT clause")
            
        # Specific suggestions based on components
        if len(components['joins']) > 3:
            suggestions.append("Multiple JOINs detected - ensure proper indexing on join columns")
            suggestions.append("Consider using EXISTS instead of JOIN where appropriate")
            
        if components['subqueries'] > 2:
            suggestions.append("Multiple subqueries detected - consider using CTEs for better readability")
            suggestions.append("Some subqueries might be convertible to JOINs for better performance")
            
        if len(components['window_functions']) > 0:
            suggestions.append("Window functions detected - ensure data is properly partitioned")
            
        if components['distinct'] and len(components['aggregate_functions']) > 0:
            suggestions.append("DISTINCT with aggregations - verify if DISTINCT is necessary")
            
        if len(components['aggregate_functions']) > 5:
            suggestions.append("Many aggregate functions - consider pre-aggregating data if this query runs frequently")
            
        if not suggestions:
            suggestions.append("Query appears well-optimized for its complexity level")
            
        return suggestions

    def analyze_csv_complexity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze complexity of a CSV dataset"""
        # Convert data types to JSON-serializable format
        data_types_counts = df.dtypes.value_counts()
        data_types_dict = {str(dtype): int(count) for dtype, count in data_types_counts.items()}
        
        # Convert null percentages to JSON-serializable format
        null_percentages = (df.isnull().sum() / len(df) * 100)
        null_percentages_dict = {str(col): float(pct) for col, pct in null_percentages.items()}
        
        stats = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': float(df.memory_usage(deep=True).sum() / (1024 * 1024)),
            'data_types': data_types_dict,
            'null_percentages': null_percentages_dict,
            'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
            'categorical_columns': df.select_dtypes(include=['object']).columns.tolist(),
            'estimated_complexity': self._estimate_dataset_complexity(df)
        }
        
        return stats

    def _estimate_dataset_complexity(self, df: pd.DataFrame) -> str:
        """Estimate overall dataset complexity"""
        score = 0
        
        # Size factors
        if len(df) > 100000:
            score += 3
        elif len(df) > 10000:
            score += 2
        elif len(df) > 1000:
            score += 1
            
        if len(df.columns) > 50:
            score += 3
        elif len(df.columns) > 20:
            score += 2
        elif len(df.columns) > 10:
            score += 1
            
        # Data type diversity
        unique_types = len(df.dtypes.value_counts())
        if unique_types > 5:
            score += 2
        elif unique_types > 3:
            score += 1
            
        # Memory usage
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        if memory_mb > 500:
            score += 3
        elif memory_mb > 100:
            score += 2
        elif memory_mb > 10:
            score += 1
            
        if score <= 2:
            return "Simple"
        elif score <= 5:
            return "Medium"
        elif score <= 8:
            return "Complex"
        else:
            return "Very Complex"