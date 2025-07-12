'use client';

import { useState, useEffect } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { logger } from '../utils/logger';
import { 
  Button, 
  Card, 
  Elevation, 
  H3, 
  H5, 
  Text, 
  Checkbox, 
  Intent, 
  Callout, 
  Classes,
  Tag,
  Divider
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';

interface SQLQueryEditorProps {
  data: any[];
  dataStats: any;
  onQueryExecuted: (results: any, analysis: any) => void;
}

interface DynamicQuery {
  name: string;
  query: string;
  description: string;
  complexity: 'basic' | 'intermediate' | 'advanced';
}

export default function SQLQueryEditor({ data, dataStats, onQueryExecuted }: SQLQueryEditorProps) {
  const [query, setQuery] = useState('SELECT * FROM data_table LIMIT 10');
  const [loading, setLoading] = useState(false);
  const [complexityAnalysis, setComplexityAnalysis] = useState<any>(null);
  const [error, setError] = useState('');
  const [autoAnalyze, setAutoAnalyze] = useState(true);
  const [dynamicQueries, setDynamicQueries] = useState<DynamicQuery[]>([]);

  // Generate dynamic queries based on data structure
  const generateDynamicQueries = (columns: string[], sampleRow: any, data: any[]): DynamicQuery[] => {
    const queries: DynamicQuery[] = [];
    
    // Find different types of columns
    const numericColumns = columns.filter(col => 
      typeof sampleRow[col] === 'number' && !isNaN(sampleRow[col])
    );
    const stringColumns = columns.filter(col => 
      typeof sampleRow[col] === 'string' && sampleRow[col] !== null
    );
    const dateColumns = columns.filter(col => 
      typeof sampleRow[col] === 'string' && 
      (col.toLowerCase().includes('date') || col.toLowerCase().includes('time') ||
       /^\d{4}-\d{2}-\d{2}/.test(sampleRow[col]))
    );
    
    // Find potential categorical columns (strings with limited unique values)
    const categoricalColumns = stringColumns.filter(col => {
      const uniqueValues = new Set(data.slice(0, 100).map(row => row[col]));
      return uniqueValues.size <= 20; // Assume categorical if ≤20 unique values in first 100 rows
    });

    // Basic queries
    queries.push({
      name: "View All Data",
      query: `SELECT * FROM data_table LIMIT 10`,
      description: "Display first 10 rows of the dataset",
      complexity: 'basic'
    });

    queries.push({
      name: "Row Count",
      query: `SELECT COUNT(*) as total_rows FROM data_table`,
      description: "Count total number of rows",
      complexity: 'basic'
    });

    // Column-specific queries
    if (numericColumns.length > 0) {
      const firstNumeric = numericColumns[0];
      queries.push({
        name: "Numeric Summary",
        query: `SELECT 
  COUNT(*) as count,
  AVG(${firstNumeric}) as avg_${firstNumeric},
  MIN(${firstNumeric}) as min_${firstNumeric},
  MAX(${firstNumeric}) as max_${firstNumeric}
FROM data_table`,
        description: `Statistical summary of ${firstNumeric}`,
        complexity: 'basic'
      });

      queries.push({
        name: "Above Average Filter",
        query: `SELECT * FROM data_table 
WHERE ${firstNumeric} > (SELECT AVG(${firstNumeric}) FROM data_table)
ORDER BY ${firstNumeric} DESC
LIMIT 20`,
        description: `Find records with above-average ${firstNumeric}`,
        complexity: 'intermediate'
      });
    }

    // Categorical analysis
    if (categoricalColumns.length > 0) {
      const firstCategorical = categoricalColumns[0];
      queries.push({
        name: "Category Distribution",
        query: `SELECT 
  ${firstCategorical}, 
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM data_table), 2) as percentage
FROM data_table 
GROUP BY ${firstCategorical} 
ORDER BY count DESC`,
        description: `Distribution analysis of ${firstCategorical}`,
        complexity: 'intermediate'
      });

      // If we have both categorical and numeric
      if (numericColumns.length > 0) {
        queries.push({
          name: "Group Analysis",
          query: `SELECT 
  ${firstCategorical},
  COUNT(*) as count,
  AVG(${numericColumns[0]}) as avg_${numericColumns[0]},
  SUM(${numericColumns[0]}) as total_${numericColumns[0]}
FROM data_table 
GROUP BY ${firstCategorical}
HAVING COUNT(*) > 1
ORDER BY avg_${numericColumns[0]} DESC`,
          description: `Analyze ${numericColumns[0]} by ${firstCategorical}`,
          complexity: 'intermediate'
        });
      }
    }

    // Date-based queries
    if (dateColumns.length > 0) {
      const firstDate = dateColumns[0];
      queries.push({
        name: "Date Range Analysis",
        query: `SELECT 
  ${firstDate},
  COUNT(*) as records
FROM data_table 
GROUP BY ${firstDate}
ORDER BY ${firstDate}
LIMIT 10`,
        description: `Analyze data distribution over ${firstDate}`,
        complexity: 'intermediate'
      });
    }

    // Advanced queries
    if (categoricalColumns.length >= 2) {
      queries.push({
        name: "Cross-Category Analysis",
        query: `SELECT 
  ${categoricalColumns[0]},
  ${categoricalColumns[1]},
  COUNT(*) as count
FROM data_table 
GROUP BY ${categoricalColumns[0]}, ${categoricalColumns[1]}
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 15`,
        description: `Cross-analysis between ${categoricalColumns[0]} and ${categoricalColumns[1]}`,
        complexity: 'advanced'
      });
    }

    // Window function example if we have numeric data
    if (numericColumns.length > 0 && categoricalColumns.length > 0) {
      queries.push({
        name: "Ranking within Groups",
        query: `SELECT 
  ${categoricalColumns[0]},
  ${numericColumns[0]},
  ROW_NUMBER() OVER (PARTITION BY ${categoricalColumns[0]} ORDER BY ${numericColumns[0]} DESC) as rank_in_group,
  ${numericColumns[0]} - AVG(${numericColumns[0]}) OVER (PARTITION BY ${categoricalColumns[0]}) as deviation_from_group_avg
FROM data_table
ORDER BY ${categoricalColumns[0]}, rank_in_group`,
        description: `Rank records within each ${categoricalColumns[0]} group`,
        complexity: 'advanced'
      });
    }

    // Null analysis
    queries.push({
      name: "Data Quality Check",
      query: `SELECT 
  ${columns.slice(0, 5).map(col => 
    `SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) as null_${col}`
  ).join(',\n  ')}
FROM data_table`,
      description: "Check for missing values in key columns",
      complexity: 'intermediate'
    });

    return queries;
  };

  useEffect(() => {
    if (data.length > 0) {
      const columns = Object.keys(data[0]);
      const sampleRow = data[0];
      const queries = generateDynamicQueries(columns, sampleRow, data);
      setDynamicQueries(queries);
      
      // Set initial query based on data
      setQuery(`SELECT * FROM data_table LIMIT 10`);
    }
  }, [data]);

  // Auto-analyze query complexity when query changes
  useEffect(() => {
    if (autoAnalyze && query.trim() && dataStats) {
      const timeoutId = setTimeout(() => {
        analyzeQueryComplexity();
      }, 1000); // Debounce for 1 second

      return () => clearTimeout(timeoutId);
    }
  }, [query, dataStats, autoAnalyze]);

  const analyzeQueryComplexity = async () => {
    if (!query.trim()) return;

    try {
      const response = await fetch('http://localhost:5002/api/analyze-query-complexity', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          data_stats: dataStats
        }),
      });

      const result = await response.json();
      if (result.success) {
        setComplexityAnalysis(result.analysis);
      }
    } catch (error) {
      logger.logApiError('analyze-query-complexity', 'POST', error, undefined, undefined);
    }
  };

  const executeQuery = async () => {
    if (!data.length) {
      alert('Please select a dataset first');
      return;
    }

    if (!query.trim()) {
      alert('Please enter a SQL query');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await fetch('http://localhost:5002/api/sql-query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          data,
          query,
          analyze_complexity: true
        }),
      });

      const result = await response.json();
      
      if (result.success) {
        onQueryExecuted(result, result.complexity_analysis);
        if (result.complexity_analysis) {
          setComplexityAnalysis(result.complexity_analysis);
        }
      } else {
        setError(result.error || 'An error occurred while executing the query');
      }
    } catch (err) {
      setError('Failed to connect to backend. Make sure the server is running.');
    } finally {
      setLoading(false);
    }
  };

  const getComplexityIntent = (rating: number): Intent => {
    if (rating <= 3) return Intent.SUCCESS;
    if (rating <= 6) return Intent.WARNING;
    if (rating <= 8) return Intent.DANGER;
    return Intent.DANGER;
  };

  const getComplexityIcon = (rating: number) => {
    if (rating <= 3) return IconNames.TICK_CIRCLE;
    if (rating <= 6) return IconNames.TIME;
    return IconNames.WARNING_SIGN;
  };

  const formatDuration = (seconds: number) => {
    if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    return `${Math.round(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  };

  return (
    <Card elevation={Elevation.TWO} style={{ 
      backgroundColor: '#394B59',
      border: '1px solid #5C7080',
      borderRadius: '8px'
    }}>
      <div style={{ padding: '24px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
          <H3 style={{ 
            margin: 0,
            color: '#F5F8FA',
            fontSize: '18px',
            fontWeight: '600'
          }}>
            SQL Query Editor
          </H3>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <Checkbox
              checked={autoAnalyze}
              onChange={(e) => setAutoAnalyze(e.target.checked)}
              label="Auto-analyze complexity"
            />
            <Button
              icon={IconNames.SEARCH}
              onClick={analyzeQueryComplexity}
              disabled={!query.trim() || !dataStats}
              text="Analyze"
            />
          </div>
        </div>

        {/* Dynamic Query Suggestions */}
        {dynamicQueries.length > 0 && (
          <div style={{ marginBottom: '24px' }}>
            <H5 style={{ 
              margin: '0 0 16px 0',
              color: '#F5F8FA',
              fontSize: '15px',
              fontWeight: '600'
            }}>
              Suggested Queries for Your Data:
            </H5>
            
            {/* Query Categories */}
            {['basic', 'intermediate', 'advanced'].map(complexity => {
              const queriesForComplexity = dynamicQueries.filter(q => q.complexity === complexity);
              if (queriesForComplexity.length === 0) return null;
              
              const complexityColors = {
                basic: '#29A634',
                intermediate: '#D9822B', 
                advanced: '#C23030'
              };
              
              return (
                <div key={complexity} style={{ marginBottom: '20px' }}>
                  <div style={{ 
                    display: 'flex', 
                    alignItems: 'center', 
                    marginBottom: '12px',
                    gap: '8px'
                  }}>
                    <Tag 
                      style={{ 
                        backgroundColor: complexityColors[complexity] + '22',
                        color: complexityColors[complexity],
                        fontSize: '11px',
                        textTransform: 'capitalize'
                      }}
                    >
                      {complexity}
                    </Tag>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      {complexity === 'basic' && 'Start here - simple data exploration'}
                      {complexity === 'intermediate' && 'Deeper analysis with grouping and filtering'}
                      {complexity === 'advanced' && 'Complex operations and window functions'}
                    </Text>
                  </div>
                  
                  <div className="sample-queries-grid" style={{ 
                    display: 'grid', 
                    gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', 
                    gap: '12px',
                    marginBottom: '16px'
                  }}>
                    {queriesForComplexity.map((sample, index) => (
                      <Card
                        key={`${complexity}-${index}`}
                        interactive={true}
                        onClick={() => setQuery(sample.query)}
                        elevation={Elevation.ONE}
                        style={{ 
                          padding: '14px', 
                          cursor: 'pointer',
                          backgroundColor: '#30404D',
                          border: `1px solid ${complexityColors[complexity]}44`,
                          borderRadius: '6px',
                          transition: 'all 0.2s ease'
                        }}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.borderColor = complexityColors[complexity];
                          e.currentTarget.style.transform = 'translateY(-2px)';
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.borderColor = complexityColors[complexity] + '44';
                          e.currentTarget.style.transform = 'translateY(0)';
                        }}
                      >
                        <Text style={{ 
                          fontSize: '14px',
                          fontWeight: '600',
                          color: '#F5F8FA',
                          marginBottom: '4px',
                          display: 'block'
                        }}>
                          {sample.name}
                        </Text>
                        <Text className={Classes.TEXT_MUTED} style={{ 
                          fontSize: '12px',
                          lineHeight: '1.4'
                        }}>
                          {sample.description}
                        </Text>
                      </Card>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Fallback when no data is loaded */}
        {dynamicQueries.length === 0 && (
          <Callout 
            icon={IconNames.INFO_SIGN}
            style={{ marginBottom: '24px' }}
          >
            <Text>
              Load a dataset first to see customized query suggestions based on your data structure.
            </Text>
          </Callout>
        )}

        {/* Complexity Analysis Preview */}
        {complexityAnalysis && (
          <Callout
            intent={getComplexityIntent(complexityAnalysis.complexity_rating)}
            icon={getComplexityIcon(complexityAnalysis.complexity_rating)}
            style={{ marginBottom: '20px' }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text className={Classes.TEXT_LARGE}>
                Complexity: {complexityAnalysis.complexity_rating}/10
              </Text>
              <Tag intent={getComplexityIntent(complexityAnalysis.complexity_rating)}>
                Est. Time: {formatDuration(complexityAnalysis.performance_estimate?.estimated_execution_time_seconds || 0)}
              </Tag>
            </div>
            
            {complexityAnalysis.optimization_suggestions && complexityAnalysis.optimization_suggestions.length > 0 && (
              <div style={{ marginTop: '10px' }}>
                <Text><strong>Suggestions:</strong> {complexityAnalysis.optimization_suggestions[0]}</Text>
              </div>
            )}
          </Callout>
        )}

        {/* Query Editor */}
        <div style={{ marginBottom: '24px' }}>
          <Card elevation={Elevation.ONE} style={{ 
            border: '1px solid #5C7080',
            borderRadius: '6px',
            overflow: 'hidden'
          }}>
            <CodeMirror
              value={query}
              height="200px"
              extensions={[sql()]}
              theme={oneDark}
              onChange={(value) => setQuery(value)}
              placeholder="Enter your SQL query here..."
            />
          </Card>
        </div>

        {/* Data Info */}
        {dataStats && (
          <Callout icon={IconNames.INFO_SIGN} style={{ marginBottom: '20px' }}>
            <Text>
              Dataset: {dataStats.row_count?.toLocaleString()} rows, {dataStats.column_count} columns
              {dataStats.memory_usage_mb && (
                <span> • Memory: {dataStats.memory_usage_mb.toFixed(1)} MB</span>
              )}
            </Text>
          </Callout>
        )}

        {/* Execute Button */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text className={Classes.TEXT_MUTED}>
            {data.length ? `Ready to query ${data.length} rows` : 'Select a dataset to begin'}
          </Text>
          <Button
            icon={IconNames.PLAY}
            onClick={executeQuery}
            disabled={loading || !data.length || !query.trim()}
            loading={loading}
            intent={Intent.PRIMARY}
            large={true}
            text={loading ? 'Executing...' : 'Execute Query'}
          />
        </div>

        {/* Error Display */}
        {error && (
          <Callout
            intent={Intent.DANGER}
            icon={IconNames.WARNING_SIGN}
            style={{ marginTop: '20px' }}
          >
            <Text>{error}</Text>
          </Callout>
        )}
      </div>
    </Card>
  );
}