'use client';

import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { 
  Button, 
  Card, 
  Elevation, 
  H3, 
  H5, 
  Text, 
  Intent, 
  Classes,
  Tag,
  Callout,
  Icon
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import ComplexityQA from './ComplexityQA';

interface ComplexityDashboardProps {
  analysis: any;
}

export default function ComplexityDashboard({ analysis }: ComplexityDashboardProps) {
  if (!analysis) return null;

  const complexityData = [
    { name: 'Compute', value: analysis.compute_score || 0, color: '#3b82f6' },
    { name: 'Memory', value: analysis.memory_score || 0, color: '#ef4444' },
  ];

  const performanceData = [
    { name: 'Execution Time', value: analysis.performance_estimate?.estimated_execution_time_seconds || 0 },
    { name: 'Result Rows', value: (analysis.performance_estimate?.estimated_result_rows || 0) / 1000 }, // Scaled for visualization
  ];

  const getComplexityIntent = (rating: number): Intent => {
    if (rating <= 3) return Intent.SUCCESS;
    if (rating <= 6) return Intent.WARNING;
    if (rating <= 8) return Intent.DANGER;
    return Intent.DANGER;
  };

  const formatDuration = (seconds: number) => {
    if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    return `${Math.round(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  };

  const MetricCard = ({ icon, title, value, intent, subtitle }: any) => (
    <Card elevation={Elevation.ONE} style={{ 
      padding: '16px',
      backgroundColor: '#30404D',
      border: '1px solid #5C7080',
      borderRadius: '6px'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <Icon icon={icon} size={18} intent={intent} />
          <Text style={{ 
            fontSize: '14px',
            fontWeight: '600',
            color: '#F5F8FA'
          }}>
            {title}
          </Text>
        </div>
        <Tag intent={intent} style={{ 
          fontSize: '13px',
          fontWeight: '600'
        }}>
          {value}
        </Tag>
      </div>
      {subtitle && (
        <Text className={Classes.TEXT_MUTED} style={{ 
          marginTop: '6px',
          fontSize: '12px'
        }}>
          {subtitle}
        </Text>
      )}
    </Card>
  );

  return (
    <Card elevation={Elevation.TWO} style={{ 
      backgroundColor: '#394B59',
      border: '1px solid #5C7080',
      borderRadius: '8px'
    }}>
      <div style={{ padding: '24px' }}>
        <H3 style={{ 
          display: 'flex', 
          alignItems: 'center', 
          marginBottom: '24px',
          color: '#F5F8FA',
          fontSize: '18px',
          fontWeight: '600'
        }}>
          <Icon icon={IconNames.BRAIN} size={20} style={{ marginRight: '12px' }} />
          Complexity Analysis
        </H3>

        {/* Overall Rating */}
        <Callout
          intent={getComplexityIntent(analysis.complexity_rating)}
          style={{ marginBottom: '20px' }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <H5 style={{ margin: 0 }}>Overall Complexity</H5>
              <Text className={Classes.TEXT_MUTED}>Combined compute and memory score</Text>
            </div>
            <Tag intent={getComplexityIntent(analysis.complexity_rating)} large={true} style={{ fontSize: '18px' }}>
              {analysis.complexity_rating}/10
            </Tag>
          </div>
        </Callout>

        {/* Key Metrics */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: '15px', marginBottom: '20px' }}>
          <MetricCard
            icon={IconNames.FLASH}
            title="Compute Score"
            value={`${analysis.compute_score || 0}/10`}
            intent={Intent.PRIMARY}
            subtitle="Processing complexity"
          />
          <MetricCard
            icon={IconNames.DATABASE}
            title="Memory Score"
            value={`${analysis.memory_score || 0}/10`}
            intent={Intent.WARNING}
            subtitle="Memory requirements"
          />
          <MetricCard
            icon={IconNames.TIME}
            title="Est. Time"
            value={formatDuration(analysis.performance_estimate?.estimated_execution_time_seconds || 0)}
            intent={Intent.SUCCESS}
            subtitle="Expected execution time"
          />
        </div>

        {/* Complexity Breakdown Chart */}
        <div style={{ marginBottom: '24px' }}>
          <H5 style={{ 
            marginBottom: '16px',
            color: '#F5F8FA',
            fontSize: '15px',
            fontWeight: '600'
          }}>
            Complexity Breakdown
          </H5>
          <Card elevation={Elevation.ONE} style={{ 
            padding: '18px',
            backgroundColor: '#30404D',
            border: '1px solid #5C7080',
            borderRadius: '6px'
          }}>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={complexityData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#5C7080" />
                <XAxis dataKey="name" tick={{ fill: '#A7B6C2', fontSize: 12 }} />
                <YAxis domain={[0, 10]} tick={{ fill: '#A7B6C2', fontSize: 12 }} />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#394B59', 
                    border: '1px solid #5C7080',
                    borderRadius: '4px',
                    color: '#F5F8FA'
                  }} 
                />
                <Bar dataKey="value" fill="#48AFF0" radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </Card>
        </div>

        {/* Query Components */}
        {analysis.components && (
          <div style={{ marginBottom: '20px' }}>
            <H5 style={{ marginBottom: '15px' }}>Query Components</H5>
            <Card elevation={Elevation.ONE} style={{ padding: '15px' }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {analysis.components.tables?.length > 0 && (
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text>Tables:</Text>
                    <Text className={Classes.TEXT_LARGE}>{analysis.components.tables.join(', ')}</Text>
                  </div>
                )}
                {analysis.components.joins?.length > 0 && (
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text>Joins:</Text>
                    <Text className={Classes.TEXT_LARGE}>{analysis.components.joins.length}</Text>
                  </div>
                )}
                {analysis.components.aggregate_functions?.length > 0 && (
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text>Aggregations:</Text>
                    <Text className={Classes.TEXT_LARGE}>{analysis.components.aggregate_functions.length}</Text>
                  </div>
                )}
                {analysis.components.subqueries > 0 && (
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text>Subqueries:</Text>
                    <Text className={Classes.TEXT_LARGE}>{analysis.components.subqueries}</Text>
                  </div>
                )}
                {analysis.components.window_functions?.length > 0 && (
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text>Window Functions:</Text>
                    <Text className={Classes.TEXT_LARGE}>{analysis.components.window_functions.length}</Text>
                  </div>
                )}
              </div>
            </Card>
          </div>
        )}

        {/* Performance Estimates */}
        {analysis.performance_estimate && (
          <div style={{ marginBottom: '20px' }}>
            <H5 style={{ marginBottom: '15px' }}>Performance Estimates</H5>
            <Card elevation={Elevation.ONE} style={{ padding: '15px' }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', paddingBottom: '10px', borderBottom: '1px solid #CED9E0' }}>
                  <Text>Execution Time:</Text>
                  <Text className={Classes.TEXT_LARGE}>
                    {formatDuration(analysis.performance_estimate.estimated_execution_time_seconds)}
                  </Text>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', paddingBottom: '10px', borderBottom: '1px solid #CED9E0' }}>
                  <Text>Result Rows:</Text>
                  <Text className={Classes.TEXT_LARGE}>
                    {analysis.performance_estimate.estimated_result_rows?.toLocaleString()}
                  </Text>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', paddingBottom: '10px', borderBottom: '1px solid #CED9E0' }}>
                  <Text>Memory Usage:</Text>
                  <Text className={Classes.TEXT_LARGE}>
                    {analysis.performance_estimate.memory_usage_category}
                  </Text>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <Text>Performance:</Text>
                  <Text className={Classes.TEXT_LARGE}>
                    {analysis.performance_estimate.performance_category}
                  </Text>
                </div>
              </div>
            </Card>
          </div>
        )}

        {/* Complexity Explanations */}
        <div style={{ marginBottom: '20px' }}>
          <H5 style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
            <Icon icon={IconNames.HELP} size={16} style={{ marginRight: '10px' }} />
            Understanding Complexity Metrics
          </H5>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <Callout intent={Intent.NONE} icon={IconNames.FLASH}>
              <div>
                <Text style={{ fontWeight: 'bold' }}>Compute Score (1-10):</Text>
                <Text> Measures processing complexity based on operations like JOINs, subqueries, and aggregations. Higher scores indicate more CPU-intensive queries.</Text>
                <br />
                <Text style={{ fontSize: '12px', fontStyle: 'italic' }}>
                  Ask: "Why is my compute score high?" or "How can I reduce compute complexity?"
                </Text>
              </div>
            </Callout>
            
            <Callout intent={Intent.NONE} icon={IconNames.DATABASE}>
              <div>
                <Text style={{ fontWeight: 'bold' }}>Memory Score (1-10):</Text>
                <Text> Estimates memory requirements based on data size and operations. Higher scores suggest the query may need significant RAM.</Text>
                <br />
                <Text style={{ fontSize: '12px', fontStyle: 'italic' }}>
                  Ask: "What affects memory usage?" or "How to optimize memory consumption?"
                </Text>
              </div>
            </Callout>
            
            <Callout intent={Intent.NONE} icon={IconNames.TIME}>
              <div>
                <Text style={{ fontWeight: 'bold' }}>Execution Time:</Text>
                <Text> Predicted time based on data size and query complexity. Actual times may vary based on hardware and data distribution.</Text>
                <br />
                <Text style={{ fontSize: '12px', fontStyle: 'italic' }}>
                  Ask: "Why is my query slow?" or "How accurate are time estimates?"
                </Text>
              </div>
            </Callout>
            
            <Callout intent={Intent.NONE} icon={IconNames.DIAGRAM_TREE}>
              <div>
                <Text style={{ fontWeight: 'bold' }}>Query Components:</Text>
                <Text> Breaking down your query shows which parts contribute most to complexity - JOINs, subqueries, window functions, etc.</Text>
                <br />
                <Text style={{ fontSize: '12px', fontStyle: 'italic' }}>
                  Ask: "What are window functions?" or "When should I use subqueries vs JOINs?"
                </Text>
              </div>
            </Callout>
          </div>
        </div>

        {/* Interactive AI Q&A Section */}
        <div style={{ marginBottom: '20px' }}>
          <H5 style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
            <Icon icon={IconNames.CHAT} size={16} style={{ marginRight: '10px' }} />
            Ask AI About Your Analysis
          </H5>
          <ComplexityQA analysis={analysis} />
        </div>

        {/* Optimization Suggestions */}
        {analysis.optimization_suggestions && analysis.optimization_suggestions.length > 0 && (
          <div>
            <H5 style={{ display: 'flex', alignItems: 'center', marginBottom: '15px' }}>
              <Icon icon={IconNames.TRENDING_UP} size={16} style={{ marginRight: '10px' }} />
              Optimization Suggestions
            </H5>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              {analysis.optimization_suggestions.map((suggestion: string, index: number) => (
                <Callout
                  key={index}
                  intent={Intent.PRIMARY}
                  icon={IconNames.LIGHTBULB}
                >
                  <Text>{suggestion}</Text>
                  <br />
                  <Text style={{ fontSize: '12px', fontStyle: 'italic', marginTop: '5px' }}>
                    Ask: "Can you explain this suggestion in more detail?"
                  </Text>
                </Callout>
              ))}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}