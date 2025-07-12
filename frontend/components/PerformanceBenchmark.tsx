'use client';

import { useState } from 'react';
import {
  Card,
  Elevation,
  Button,
  Intent,
  FormGroup,
  HTMLSelect,
  NumericInput,
  H3,
  H5,
  Text,
  Classes,
  Callout,
  ProgressBar,
  Tag,
  Divider
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, BarChart, Bar } from 'recharts';

interface BenchmarkResult {
  spark: {
    execution_time: number;
    result_size: number;
  };
  pandas: {
    execution_time: number;
    result_size: number;
  };
  comparison: {
    speedup: number;
    winner: string;
    data_size: number;
    operation: string;
  };
}

export default function PerformanceBenchmark() {
  const [operation, setOperation] = useState('aggregation');
  const [dataSize, setDataSize] = useState(100000);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<BenchmarkResult | null>(null);
  const [benchmarkHistory, setBenchmarkHistory] = useState<any[]>([]);
  const [error, setError] = useState<string>('');

  const operations = [
    { value: 'aggregation', label: 'Group By Aggregations' },
    { value: 'filtering', label: 'Data Filtering' },
    { value: 'join', label: 'Table Joins' },
    { value: 'count', label: 'Row Counting' }
  ];

  const dataSizes = [
    { value: 10000, label: '10K rows' },
    { value: 50000, label: '50K rows' },
    { value: 100000, label: '100K rows' },
    { value: 250000, label: '250K rows' },
    { value: 500000, label: '500K rows' },
    { value: 1000000, label: '1M rows' }
  ];

  const runBenchmark = async () => {
    setLoading(true);
    setError('');
    
    try {
      const response = await fetch('/api/performance-comparison', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          operation: operation,
          data_size: dataSize
        }),
      });

      const result = await response.json();
      
      if (result.success) {
        setResults(result.results);
        
        // Add to history for chart
        const historyEntry = {
          operation: operation,
          dataSize: dataSize,
          sparkTime: result.results.spark.execution_time,
          pandasTime: result.results.pandas.execution_time,
          speedup: result.results.comparison.speedup,
          winner: result.results.comparison.winner,
          timestamp: new Date().toLocaleTimeString()
        };
        
        setBenchmarkHistory(prev => [...prev.slice(-9), historyEntry]); // Keep last 10 results
      } else {
        setError(result.error);
      }
    } catch (err) {
      setError('Failed to run benchmark: ' + (err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const getSpeedupColor = (speedup: number) => {
    if (speedup > 1.5) return '#29A634'; // Green for Spark advantage
    if (speedup > 0.8) return '#D9822B'; // Orange for close
    return '#C23030'; // Red for Pandas advantage
  };

  const formatTime = (seconds: number) => {
    if (seconds < 0.001) return `${(seconds * 1000000).toFixed(0)}μs`;
    if (seconds < 1) return `${(seconds * 1000).toFixed(1)}ms`;
    return `${seconds.toFixed(3)}s`;
  };

  return (
    <div style={{ padding: '32px', maxWidth: '1400px', margin: '0 auto' }}>
      <div style={{ marginBottom: '32px', textAlign: 'center' }}>
        <H3 style={{ marginBottom: '8px', color: '#F5F8FA' }}>Performance Benchmark</H3>
        <Text style={{ color: '#A7B6C2', fontSize: '16px' }}>
          Compare Spark vs Pandas performance across different operations and data sizes
        </Text>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '32px' }}>
        {/* Controls Panel */}
        <Card elevation={Elevation.TWO} style={{ 
          backgroundColor: '#30404D',
          border: '1px solid #394B59',
          padding: '24px'
        }}>
          <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>Benchmark Configuration</H5>
          
          <FormGroup
            label="Operation Type"
            labelInfo="(required)"
          >
            <HTMLSelect
              value={operation}
              onChange={(e) => setOperation(e.currentTarget.value)}
              fill={true}
              options={operations}
            />
          </FormGroup>

          <FormGroup
            label="Dataset Size"
            labelInfo="(rows)"
          >
            <HTMLSelect
              value={dataSize}
              onChange={(e) => setDataSize(Number(e.currentTarget.value))}
              fill={true}
              options={dataSizes}
            />
          </FormGroup>

          <Button
            intent={Intent.PRIMARY}
            fill={true}
            large={true}
            loading={loading}
            onClick={runBenchmark}
            icon={IconNames.PLAY}
            style={{ marginTop: '20px' }}
          >
            {loading ? 'Running Benchmark...' : 'Run Benchmark'}
          </Button>

          {loading && (
            <div style={{ marginTop: '16px' }}>
              <ProgressBar intent={Intent.PRIMARY} />
              <Text style={{ 
                fontSize: '12px', 
                color: '#A7B6C2', 
                textAlign: 'center',
                marginTop: '8px'
              }}>
                Processing {dataSize.toLocaleString()} rows...
              </Text>
            </div>
          )}

          {error && (
            <Callout
              intent={Intent.DANGER}
              title="Benchmark Failed"
              style={{ marginTop: '16px' }}
            >
              {error}
            </Callout>
          )}

          {/* Operation Info */}
          <Card elevation={Elevation.ONE} style={{ 
            marginTop: '24px',
            backgroundColor: '#394B59',
            padding: '16px'
          }}>
            <H5 style={{ marginBottom: '12px', fontSize: '14px', color: '#F5F8FA' }}>
              What This Tests
            </H5>
            <Text style={{ fontSize: '12px', color: '#A7B6C2', lineHeight: '1.4' }}>
              {operation === 'aggregation' && 'Group by operations with mean, count, and sum calculations across multiple categories.'}
              {operation === 'filtering' && 'Filtering operations to find rows matching specific conditions.'}
              {operation === 'join' && 'Inner join operations between two datasets on a common key.'}
              {operation === 'count' && 'Simple row counting operations across the entire dataset.'}
            </Text>
          </Card>
        </Card>

        {/* Results Panel */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          {/* Current Result */}
          {results && (
            <Card elevation={Elevation.TWO} style={{ 
              backgroundColor: '#30404D',
              border: '1px solid #394B59',
              padding: '24px'
            }}>
              <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>Latest Benchmark Results</H5>
              
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px', marginBottom: '20px' }}>
                {/* Spark Results */}
                <div style={{ 
                  backgroundColor: '#394B59', 
                  padding: '16px', 
                  borderRadius: '8px',
                  border: results.comparison.winner === 'Spark' ? '2px solid #29A634' : '1px solid #5C7080'
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                    <span style={{ fontSize: '18px', marginRight: '8px' }}>⚡</span>
                    <Text style={{ fontWeight: 'bold', color: '#F5F8FA' }}>Apache Spark</Text>
                    {results.comparison.winner === 'Spark' && (
                      <Tag intent={Intent.SUCCESS} minimal style={{ marginLeft: '8px' }}>Winner</Tag>
                    )}
                  </div>
                  <Text style={{ fontSize: '24px', fontWeight: 'bold', color: '#48AFF0' }}>
                    {formatTime(results.spark.execution_time)}
                  </Text>
                </div>

                {/* Pandas Results */}
                <div style={{ 
                  backgroundColor: '#394B59', 
                  padding: '16px', 
                  borderRadius: '8px',
                  border: results.comparison.winner === 'Pandas' ? '2px solid #29A634' : '1px solid #5C7080'
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                    <span style={{ fontSize: '18px', marginRight: '8px' }}>🐼</span>
                    <Text style={{ fontWeight: 'bold', color: '#F5F8FA' }}>Pandas</Text>
                    {results.comparison.winner === 'Pandas' && (
                      <Tag intent={Intent.SUCCESS} minimal style={{ marginLeft: '8px' }}>Winner</Tag>
                    )}
                  </div>
                  <Text style={{ fontSize: '24px', fontWeight: 'bold', color: '#D9822B' }}>
                    {formatTime(results.pandas.execution_time)}
                  </Text>
                </div>
              </div>

              {/* Speedup Indicator */}
              <div style={{ 
                backgroundColor: '#394B59', 
                padding: '16px', 
                borderRadius: '8px',
                textAlign: 'center'
              }}>
                <Text style={{ color: '#A7B6C2', marginBottom: '8px' }}>Performance Ratio</Text>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '12px' }}>
                  <Text style={{ 
                    fontSize: '32px', 
                    fontWeight: 'bold',
                    color: getSpeedupColor(results.comparison.speedup)
                  }}>
                    {results.comparison.speedup.toFixed(2)}x
                  </Text>
                  <Text style={{ color: '#A7B6C2' }}>
                    {results.comparison.speedup > 1 
                      ? 'Spark faster than Pandas' 
                      : 'Pandas faster than Spark'}
                  </Text>
                </div>
              </div>

              <Divider style={{ margin: '20px 0' }} />
              
              <Callout
                intent={results.comparison.speedup > 1 ? Intent.SUCCESS : Intent.WARNING}
                icon={results.comparison.speedup > 1 ? IconNames.LIGHTNING : IconNames.INFO_SIGN}
              >
                <strong>Analysis:</strong> {results.comparison.winner} performed better for {operation} 
                on {dataSize.toLocaleString()} rows. 
                {results.comparison.speedup < 1 && dataSize < 500000 && 
                  " Spark overhead is more noticeable on smaller datasets."}
                {results.comparison.speedup > 2 && 
                  " Spark's distributed processing shows significant advantages."}
              </Callout>
            </Card>
          )}

          {/* Performance History Chart */}
          {benchmarkHistory.length > 0 && (
            <Card elevation={Elevation.TWO} style={{ 
              backgroundColor: '#30404D',
              border: '1px solid #394B59',
              padding: '24px'
            }}>
              <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>Performance History</H5>
              
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={benchmarkHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#5C7080" />
                  <XAxis 
                    dataKey="timestamp" 
                    stroke="#A7B6C2"
                    fontSize={12}
                  />
                  <YAxis 
                    stroke="#A7B6C2"
                    fontSize={12}
                    label={{ value: 'Execution Time (s)', angle: -90, position: 'insideLeft' }}
                  />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: '#394B59', 
                      border: '1px solid #5C7080',
                      borderRadius: '6px'
                    }}
                    formatter={(value, name) => [
                      `${formatTime(value as number)}`,
                      name === 'sparkTime' ? 'Spark' : 'Pandas'
                    ]}
                    labelFormatter={(label) => `Time: ${label}`}
                  />
                  <Legend />
                  <Line 
                    type="monotone" 
                    dataKey="sparkTime" 
                    stroke="#48AFF0" 
                    strokeWidth={2}
                    name="Spark"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="pandasTime" 
                    stroke="#D9822B" 
                    strokeWidth={2}
                    name="Pandas"
                  />
                </LineChart>
              </ResponsiveContainer>

              {/* Data Size vs Performance Chart */}
              <div style={{ marginTop: '24px' }}>
                <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Speedup by Data Size</H5>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={benchmarkHistory}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#5C7080" />
                    <XAxis 
                      dataKey="dataSize" 
                      stroke="#A7B6C2"
                      fontSize={12}
                      tickFormatter={(value) => `${(value / 1000).toFixed(0)}K`}
                    />
                    <YAxis 
                      stroke="#A7B6C2"
                      fontSize={12}
                      label={{ value: 'Speedup (x)', angle: -90, position: 'insideLeft' }}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: '#394B59', 
                        border: '1px solid #5C7080',
                        borderRadius: '6px'
                      }}
                      formatter={(value) => [`${(value as number).toFixed(2)}x`, 'Speedup']}
                      labelFormatter={(label) => `Data Size: ${(label / 1000).toFixed(0)}K rows`}
                    />
                    <Bar 
                      dataKey="speedup" 
                      fill={(entry) => getSpeedupColor(entry.speedup)}
                      radius={[4, 4, 0, 0]}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </Card>
          )}

          {/* Performance Tips */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            border: '1px solid #5C7080',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              Performance Insights
            </H5>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Small datasets (&lt;100K rows):</strong> Pandas often faster due to lower overhead
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Large datasets (&gt;1M rows):</strong> Spark advantages become clear
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Complex operations:</strong> Spark's query optimization shines
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Memory caching:</strong> Repeated operations benefit from Spark's caching
              </Text>
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}