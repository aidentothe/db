'use client';

import { useState } from 'react';
import {
  Card,
  Elevation,
  Button,
  Intent,
  FormGroup,
  NumericInput,
  H3,
  H5,
  Text,
  Classes,
  Callout,
  ProgressBar,
  Tag,
  Checkbox,
  Divider,
  HTMLTable
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { PieChart, Pie, Cell, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, LineChart, Line, CartesianGrid } from 'recharts';

interface DatasetInfo {
  row_count: number;
  schema: Array<{ name: string; type: string }>;
  partitions: number;
  generation_time: number;
  cached: boolean;
}

interface GenerationResult {
  dataset_info: DatasetInfo;
  preview: any[];
  message: string;
}

export default function LargeDatasetGenerator() {
  const [numRows, setNumRows] = useState(100000);
  const [numPartitions, setNumPartitions] = useState(4);
  const [includeNulls, setIncludeNulls] = useState(true);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [results, setResults] = useState<GenerationResult | null>(null);
  const [generationHistory, setGenerationHistory] = useState<any[]>([]);
  const [error, setError] = useState<string>('');

  const presetSizes = [
    { label: '10K - Small', value: 10000, color: '#29A634' },
    { label: '100K - Medium', value: 100000, color: '#D9822B' },
    { label: '500K - Large', value: 500000, color: '#C23030' },
    { label: '1M - Very Large', value: 1000000, color: '#AD7BE9' }
  ];

  const generateDataset = async () => {
    setLoading(true);
    setError('');
    setProgress(0);
    
    // Simulate progress updates
    const progressInterval = setInterval(() => {
      setProgress(prev => Math.min(prev + 10, 90));
    }, 200);

    try {
      const response = await fetch('/api/generate-large-dataset', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          num_rows: numRows,
          num_partitions: numPartitions,
          include_nulls: includeNulls
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Server returned non-JSON response. Make sure the backend is running.');
      }

      const result = await response.json();
      
      if (result.success) {
        setProgress(100);
        setResults(result);
        
        // Add to generation history
        const historyEntry = {
          rows: numRows,
          partitions: numPartitions,
          generationTime: result.dataset_info.generation_time,
          throughput: numRows / result.dataset_info.generation_time,
          timestamp: new Date().toLocaleTimeString()
        };
        
        setGenerationHistory(prev => [...prev.slice(-9), historyEntry]);
      } else {
        setError(result.error);
      }
    } catch (err) {
      setError('Failed to generate dataset: ' + (err as Error).message);
    } finally {
      clearInterval(progressInterval);
      setLoading(false);
      setTimeout(() => setProgress(0), 1000);
    }
  };

  const formatNumber = (num: number) => {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
    return num.toString();
  };

  const formatTime = (seconds: number) => {
    if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
    return `${seconds.toFixed(2)}s`;
  };

  const calculateThroughput = (rows: number, time: number) => {
    return Math.round(rows / time);
  };

  const getDataTypeColor = (type: string) => {
    const colors: { [key: string]: string } = {
      'string': '#48AFF0',
      'long': '#29A634',
      'double': '#D9822B',
      'boolean': '#AD7BE9',
      'timestamp': '#C23030'
    };
    return colors[type.toLowerCase()] || '#8A9BA8';
  };

  return (
    <div style={{ padding: '32px', maxWidth: '1400px', margin: '0 auto' }}>
      <div style={{ marginBottom: '32px', textAlign: 'center' }}>
        <H3 style={{ marginBottom: '8px', color: '#F5F8FA' }}>Large Dataset Generator</H3>
        <Text style={{ color: '#A7B6C2', fontSize: '16px' }}>
          Generate massive synthetic datasets using Spark's distributed computing power
        </Text>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '32px' }}>
        {/* Controls Panel */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          <Card elevation={Elevation.TWO} style={{ 
            backgroundColor: '#30404D',
            border: '1px solid #394B59',
            padding: '24px'
          }}>
            <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>Dataset Configuration</H5>
            
            {/* Preset Buttons */}
            <FormGroup label="Quick Presets">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                {presetSizes.map((preset) => (
                  <Button
                    key={preset.value}
                    text={preset.label}
                    onClick={() => setNumRows(preset.value)}
                    intent={numRows === preset.value ? Intent.PRIMARY : Intent.NONE}
                    style={{ 
                      fontSize: '12px',
                      borderColor: preset.color + '44',
                      color: numRows === preset.value ? '#FFF' : preset.color
                    }}
                  />
                ))}
              </div>
            </FormGroup>

            <FormGroup
              label="Number of Rows"
              labelInfo="(10K - 1M)"
            >
              <NumericInput
                value={numRows}
                onValueChange={(value) => setNumRows(value)}
                min={10000}
                max={1000000}
                stepSize={10000}
                fill={true}
                majorStepSize={100000}
                id="numRows-input"
              />
            </FormGroup>

            <FormGroup
              label="Partitions"
              labelInfo="(parallelism)"
            >
              <NumericInput
                value={numPartitions}
                onValueChange={(value) => setNumPartitions(value)}
                min={1}
                max={16}
                stepSize={1}
                fill={true}
                id="partitions-input"
              />
            </FormGroup>

            <FormGroup>
              <Checkbox
                checked={includeNulls}
                onChange={(e) => setIncludeNulls(e.currentTarget.checked)}
                label="Include NULL values (realistic data)"
              />
            </FormGroup>

            <Button
              intent={Intent.PRIMARY}
              fill={true}
              large={true}
              loading={loading}
              onClick={generateDataset}
              icon={IconNames.DATABASE}
              style={{ marginTop: '20px' }}
            >
              {loading ? 'Generating Dataset...' : 'Generate Dataset'}
            </Button>

            {loading && (
              <div style={{ marginTop: '16px' }}>
                <ProgressBar 
                  intent={Intent.PRIMARY} 
                  value={progress / 100}
                  animate={true}
                />
                <Text style={{ 
                  fontSize: '12px', 
                  color: '#A7B6C2', 
                  textAlign: 'center',
                  marginTop: '8px'
                }}>
                  Creating {formatNumber(numRows)} rows across {numPartitions} partitions...
                </Text>
              </div>
            )}

            {error && (
              <Callout
                intent={Intent.DANGER}
                title="Generation Failed"
                style={{ marginTop: '16px' }}
              >
                {error}
              </Callout>
            )}
          </Card>

          {/* Expected Performance Card */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            border: '1px solid #5C7080',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              Expected Performance
            </H5>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>Est. Generation Time:</Text>
                <Text style={{ fontSize: '13px', color: '#F5F8FA', fontWeight: 'bold' }}>
                  {formatTime(numRows / 50000)} {/* Rough estimate */}
                </Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>Memory Usage:</Text>
                <Text style={{ fontSize: '13px', color: '#F5F8FA', fontWeight: 'bold' }}>
                  ~{Math.round(numRows * 0.001)}MB
                </Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>Columns:</Text>
                <Text style={{ fontSize: '13px', color: '#F5F8FA', fontWeight: 'bold' }}>
                  15 (mixed types)
                </Text>
              </div>
            </div>
          </Card>
        </div>

        {/* Results Panel */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          {/* Generation Results */}
          {results && (
            <Card elevation={Elevation.TWO} style={{ 
              backgroundColor: '#30404D',
              border: '1px solid #394B59',
              padding: '24px'
            }}>
              <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>Generation Results</H5>
              
              {/* Key Metrics */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '16px', marginBottom: '20px' }}>
                <div style={{ backgroundColor: '#394B59', padding: '16px', borderRadius: '8px', textAlign: 'center' }}>
                  <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>ROWS GENERATED</Text>
                  <Text style={{ fontSize: '24px', fontWeight: 'bold', color: '#29A634' }}>
                    {formatNumber(results.dataset_info.row_count)}
                  </Text>
                </div>
                <div style={{ backgroundColor: '#394B59', padding: '16px', borderRadius: '8px', textAlign: 'center' }}>
                  <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>GENERATION TIME</Text>
                  <Text style={{ fontSize: '24px', fontWeight: 'bold', color: '#48AFF0' }}>
                    {formatTime(results.dataset_info.generation_time)}
                  </Text>
                </div>
                <div style={{ backgroundColor: '#394B59', padding: '16px', borderRadius: '8px', textAlign: 'center' }}>
                  <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>THROUGHPUT</Text>
                  <Text style={{ fontSize: '24px', fontWeight: 'bold', color: '#D9822B' }}>
                    {formatNumber(calculateThroughput(results.dataset_info.row_count, results.dataset_info.generation_time))}/s
                  </Text>
                </div>
              </div>

              <Callout
                intent={Intent.SUCCESS}
                icon={IconNames.TICK_CIRCLE}
                style={{ marginBottom: '20px' }}
              >
                {results.message} Dataset is cached in memory for fast subsequent operations.
              </Callout>

              {/* Schema Information */}
              <div style={{ marginBottom: '20px' }}>
                <H5 style={{ marginBottom: '12px', color: '#F5F8FA', fontSize: '16px' }}>
                  Schema ({results.dataset_info.schema.length} columns)
                </H5>
                <div style={{ 
                  display: 'grid', 
                  gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                  gap: '8px'
                }}>
                  {results.dataset_info.schema.map((field, index) => (
                    <div 
                      key={index}
                      style={{ 
                        backgroundColor: '#394B59', 
                        padding: '8px 12px', 
                        borderRadius: '6px',
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center'
                      }}
                    >
                      <Text style={{ fontSize: '13px', color: '#F5F8FA' }}>
                        {field.name}
                      </Text>
                      <Tag 
                        minimal 
                        style={{ 
                          backgroundColor: getDataTypeColor(field.type) + '22',
                          color: getDataTypeColor(field.type),
                          fontSize: '10px'
                        }}
                      >
                        {field.type}
                      </Tag>
                    </div>
                  ))}
                </div>
              </div>

              {/* Data Preview */}
              <div>
                <H5 style={{ marginBottom: '12px', color: '#F5F8FA', fontSize: '16px' }}>
                  Data Preview (First 5 rows)
                </H5>
                <div style={{ 
                  backgroundColor: '#394B59', 
                  borderRadius: '8px', 
                  overflow: 'hidden',
                  border: '1px solid #5C7080'
                }}>
                  <HTMLTable
                    condensed
                    striped
                    style={{ 
                      width: '100%', 
                      margin: 0,
                      fontSize: '12px'
                    }}
                  >
                    <thead>
                      <tr style={{ backgroundColor: '#30404D' }}>
                        {Object.keys(results.preview[0] || {}).slice(0, 6).map((column) => (
                          <th key={column} style={{ 
                            padding: '8px', 
                            color: '#F5F8FA',
                            borderBottom: '1px solid #5C7080'
                          }}>
                            {column}
                          </th>
                        ))}
                        {Object.keys(results.preview[0] || {}).length > 6 && (
                          <th style={{ 
                            padding: '8px', 
                            color: '#A7B6C2',
                            borderBottom: '1px solid #5C7080',
                            textAlign: 'center'
                          }}>
                            +{Object.keys(results.preview[0] || {}).length - 6} more
                          </th>
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {results.preview.slice(0, 5).map((row, index) => (
                        <tr key={index}>
                          {Object.values(row).slice(0, 6).map((value, cellIndex) => (
                            <td key={cellIndex} style={{ 
                              padding: '6px 8px', 
                              color: '#A7B6C2',
                              borderBottom: '1px solid #5C7080'
                            }}>
                              {value === null ? (
                                <span style={{ color: '#8A9BA8', fontStyle: 'italic' }}>null</span>
                              ) : (
                                String(value).length > 20 
                                  ? String(value).substring(0, 17) + '...'
                                  : String(value)
                              )}
                            </td>
                          ))}
                          {Object.values(row).length > 6 && (
                            <td style={{ 
                              padding: '6px 8px', 
                              color: '#8A9BA8',
                              borderBottom: '1px solid #5C7080',
                              textAlign: 'center',
                              fontStyle: 'italic'
                            }}>
                              ...
                            </td>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </HTMLTable>
                </div>
              </div>
            </Card>
          )}

          {/* Performance History */}
          {generationHistory.length > 0 && (
            <Card elevation={Elevation.TWO} style={{ 
              backgroundColor: '#30404D',
              border: '1px solid #394B59',
              padding: '24px'
            }}>
              <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>Generation Performance History</H5>
              
              <ResponsiveContainer width="100%" height={250}>
                <LineChart data={generationHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#5C7080" />
                  <XAxis 
                    dataKey="timestamp" 
                    stroke="#A7B6C2"
                    fontSize={12}
                  />
                  <YAxis 
                    stroke="#A7B6C2"
                    fontSize={12}
                    label={{ value: 'Throughput (rows/sec)', angle: -90, position: 'insideLeft' }}
                  />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: '#394B59', 
                      border: '1px solid #5C7080',
                      borderRadius: '6px'
                    }}
                    formatter={(value, name) => [
                      `${formatNumber(value as number)}/s`,
                      'Throughput'
                    ]}
                    labelFormatter={(label) => `Time: ${label}`}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="throughput" 
                    stroke="#29A634" 
                    strokeWidth={3}
                    dot={{ fill: '#29A634', strokeWidth: 2, r: 4 }}
                  />
                </LineChart>
              </ResponsiveContainer>

              <div style={{ marginTop: '20px' }}>
                <H5 style={{ marginBottom: '12px', color: '#F5F8FA', fontSize: '14px' }}>
                  Dataset Size vs Generation Time
                </H5>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={generationHistory}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#5C7080" />
                    <XAxis 
                      dataKey="rows" 
                      stroke="#A7B6C2"
                      fontSize={12}
                      tickFormatter={(value) => formatNumber(value)}
                    />
                    <YAxis 
                      stroke="#A7B6C2"
                      fontSize={12}
                      label={{ value: 'Time (seconds)', angle: -90, position: 'insideLeft' }}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: '#394B59', 
                        border: '1px solid #5C7080',
                        borderRadius: '6px'
                      }}
                      formatter={(value) => [formatTime(value as number), 'Generation Time']}
                      labelFormatter={(label) => `Rows: ${formatNumber(label)}`}
                    />
                    <Bar 
                      dataKey="generationTime" 
                      fill="#48AFF0"
                      radius={[4, 4, 0, 0]}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </Card>
          )}

          {/* Why Large Datasets Matter */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            border: '1px solid #5C7080',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              Why Large Datasets Showcase Spark's Power
            </H5>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Distributed Generation:</strong> Data created in parallel across multiple partitions
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Memory Management:</strong> Automatic spill-to-disk for datasets larger than RAM
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Lazy Evaluation:</strong> Optimized execution plans for complex transformations
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Fault Tolerance:</strong> Automatic recovery from node failures during processing
              </Text>
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}