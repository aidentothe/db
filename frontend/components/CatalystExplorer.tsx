'use client';

import { useState } from 'react';
import {
  Card,
  Elevation,
  Button,
  Intent,
  H3,
  H5,
  Text,
  Classes,
  Callout,
  Tab,
  Tabs,
  Tag,
  Tree,
  TreeNodeInfo,
  Icon,
  HTMLTable,
  Divider
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';

interface CatalystRule {
  rule_name: string;
  description: string;
  applied: boolean;
  impact: string;
}

interface PlanAnalysis {
  logical_plan: any[];
  optimized_plan: any[];
  physical_plan: any[];
}

interface OptimizationSuggestion {
  type: string;
  description: string;
  impact: string;
  suggestion: string;
}

interface PerformanceEstimation {
  complexity_score: number;
  estimated_execution_time: number;
  bottleneck_prediction: {
    type: string;
    description: string;
  };
  scaling_characteristics: {
    scaling: string;
    description: string;
  };
}

interface CatalystAnalysis {
  plan_analysis: PlanAnalysis;
  catalyst_rules: CatalystRule[];
  optimization_analysis: OptimizationSuggestion[];
  performance_estimation: PerformanceEstimation;
  explain_output: string;
  query_complexity: number;
}

export default function CatalystExplorer() {
  const [query, setQuery] = useState(`SELECT category, 
  COUNT(*) as transaction_count,
  AVG(price * quantity) as avg_revenue,
  SUM(price * quantity) as total_revenue
FROM data_table 
WHERE price > 100
GROUP BY category
HAVING COUNT(*) > 5
ORDER BY total_revenue DESC`);
  
  const [selectedData, setSelectedData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [analysis, setAnalysis] = useState<CatalystAnalysis | null>(null);
  const [error, setError] = useState<string>('');
  const [activeTab, setActiveTab] = useState('overview');

  const explainQuery = async () => {
    if (!query.trim()) {
      setError('Please enter a SQL query');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/catalyst-explain', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: query,
          data: selectedData
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
        setAnalysis(result);
      } else {
        setError(result.error);
      }
    } catch (err) {
      setError('Failed to analyze query: ' + (err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const generateSampleData = () => {
    // Generate sample e-commerce data
    const sampleData = [];
    const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
    
    for (let i = 0; i < 1000; i++) {
      sampleData.push({
        id: i + 1,
        category: categories[Math.floor(Math.random() * categories.length)],
        price: Math.round((Math.random() * 500 + 10) * 100) / 100,
        quantity: Math.floor(Math.random() * 10) + 1,
        customer_id: Math.floor(Math.random() * 100) + 1,
        order_date: new Date(2024, Math.floor(Math.random() * 12), Math.floor(Math.random() * 28) + 1).toISOString().split('T')[0]
      });
    }
    
    setSelectedData(sampleData);
  };

  const getComplexityColor = (score: number) => {
    if (score <= 2) return '#29A634';
    if (score <= 5) return '#D9822B';
    return '#C23030';
  };

  const getCostColor = (cost: string) => {
    switch (cost) {
      case 'low': return '#29A634';
      case 'medium': return '#D9822B';
      case 'high': return '#C23030';
      default: return '#8A9BA8';
    }
  };

  const buildPlanTree = (planNodes: any[]): TreeNodeInfo[] => {
    return planNodes.map((node, index) => ({
      id: index,
      label: (
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Icon icon={IconNames.FLOW_BRANCH} size={14} />
          <Text style={{ fontFamily: 'monospace', fontSize: '13px' }}>
            {node.operation || node.details}
          </Text>
          {node.cost_estimate && (
            <Tag 
              minimal 
              style={{ 
                backgroundColor: getCostColor(node.cost_estimate.cost) + '22',
                color: getCostColor(node.cost_estimate.cost),
                fontSize: '10px'
              }}
            >
              {node.cost_estimate.cost}
            </Tag>
          )}
        </div>
      ),
      icon: node.type === 'logical' ? IconNames.DIAGRAM_TREE : 
            node.type === 'physical' ? IconNames.COG : IconNames.LIGHTBULB,
      isExpanded: true
    }));
  };

  const OverviewPanel = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {/* Query Complexity Overview */}
      {analysis && (
        <Card elevation={Elevation.ONE} style={{ 
          backgroundColor: '#394B59',
          padding: '20px'
        }}>
          <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Query Analysis Overview</H5>
          
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '16px', marginBottom: '20px' }}>
            <div style={{ textAlign: 'center' }}>
              <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>COMPLEXITY SCORE</Text>
              <Text style={{ 
                fontSize: '24px', 
                fontWeight: 'bold', 
                color: getComplexityColor(analysis.query_complexity)
              }}>
                {analysis.query_complexity}/10
              </Text>
            </div>
            <div style={{ textAlign: 'center' }}>
              <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>EST. EXECUTION TIME</Text>
              <Text style={{ fontSize: '24px', fontWeight: 'bold', color: '#48AFF0' }}>
                {analysis.performance_estimation.estimated_execution_time}s
              </Text>
            </div>
            <div style={{ textAlign: 'center' }}>
              <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>SCALING</Text>
              <Text style={{ fontSize: '16px', fontWeight: 'bold', color: '#D9822B' }}>
                {analysis.performance_estimation.scaling_characteristics.scaling.toUpperCase()}
              </Text>
            </div>
          </div>

          <Callout
            intent={analysis.performance_estimation.bottleneck_prediction.type === 'io' ? Intent.SUCCESS : Intent.WARNING}
            icon={IconNames.INFO_SIGN}
          >
            <strong>Predicted Bottleneck:</strong> {analysis.performance_estimation.bottleneck_prediction.description}
          </Callout>
        </Card>
      )}

      {/* Catalyst Rules Applied */}
      {analysis && (
        <Card elevation={Elevation.ONE} style={{ 
          backgroundColor: '#394B59',
          padding: '20px'
        }}>
          <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Catalyst Optimization Rules</H5>
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {analysis.catalyst_rules.map((rule, index) => (
              <div 
                key={index}
                style={{ 
                  display: 'flex', 
                  justifyContent: 'space-between', 
                  alignItems: 'center',
                  padding: '12px',
                  backgroundColor: rule.applied ? '#29A63422' : '#30404D',
                  borderRadius: '6px',
                  border: `1px solid ${rule.applied ? '#29A634' : '#5C7080'}`
                }}
              >
                <div>
                  <Text style={{ 
                    fontWeight: 'bold', 
                    color: '#F5F8FA',
                    marginBottom: '4px'
                  }}>
                    {rule.rule_name}
                  </Text>
                  <Text style={{ 
                    fontSize: '12px', 
                    color: '#A7B6C2',
                    display: 'block',
                    marginBottom: '4px'
                  }}>
                    {rule.description}
                  </Text>
                  <Text style={{ 
                    fontSize: '11px', 
                    color: '#8A9BA8',
                    fontStyle: 'italic'
                  }}>
                    Impact: {rule.impact}
                  </Text>
                </div>
                <Tag 
                  intent={rule.applied ? Intent.SUCCESS : Intent.NONE}
                  icon={rule.applied ? IconNames.TICK : IconNames.CROSS}
                >
                  {rule.applied ? 'Applied' : 'Not Applied'}
                </Tag>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Optimization Suggestions */}
      {analysis && analysis.optimization_analysis.length > 0 && (
        <Card elevation={Elevation.ONE} style={{ 
          backgroundColor: '#394B59',
          padding: '20px'
        }}>
          <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Optimization Opportunities</H5>
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {analysis.optimization_analysis.map((opt, index) => (
              <Callout
                key={index}
                intent={opt.impact === 'high' ? Intent.WARNING : Intent.PRIMARY}
                icon={opt.impact === 'high' ? IconNames.WARNING_SIGN : IconNames.LIGHTBULB}
              >
                <strong>{opt.type.replace('_', ' ').toUpperCase()}:</strong> {opt.description}
                <br />
                <em>{opt.suggestion}</em>
              </Callout>
            ))}
          </div>
        </Card>
      )}
    </div>
  );

  const ExecutionPlansPanel = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {analysis && (
        <>
          {/* Logical Plan */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              <Icon icon={IconNames.DIAGRAM_TREE} style={{ marginRight: '8px' }} />
              Logical Plan (Parsed)
            </H5>
            <Text style={{ color: '#A7B6C2', fontSize: '13px', marginBottom: '16px' }}>
              Initial query representation as parsed by Spark SQL
            </Text>
            
            {Array.isArray(analysis.plan_analysis.logical_plan) ? (
              <Tree
                contents={buildPlanTree(analysis.plan_analysis.logical_plan)}
                className="execution-plan-tree"
              />
            ) : (
              <Card elevation={Elevation.ZERO} style={{ 
                backgroundColor: '#30404D',
                padding: '16px',
                fontFamily: 'monospace',
                fontSize: '12px',
                color: '#A7B6C2'
              }}>
                {analysis.plan_analysis.logical_plan}
              </Card>
            )}
          </Card>

          {/* Optimized Plan */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              <Icon icon={IconNames.LIGHTBULB} style={{ marginRight: '8px' }} />
              Optimized Logical Plan
            </H5>
            <Text style={{ color: '#A7B6C2', fontSize: '13px', marginBottom: '16px' }}>
              Plan after Catalyst optimizer rule applications
            </Text>
            
            {Array.isArray(analysis.plan_analysis.optimized_plan) ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {analysis.plan_analysis.optimized_plan.map((opt, index) => (
                  <div 
                    key={index}
                    style={{ 
                      padding: '12px',
                      backgroundColor: '#30404D',
                      borderRadius: '6px',
                      border: '1px solid #5C7080'
                    }}
                  >
                    <Text style={{ fontFamily: 'monospace', fontSize: '12px', color: '#F5F8FA' }}>
                      {opt.operation}
                    </Text>
                    <Text style={{ fontSize: '11px', color: '#A7B6C2', marginTop: '4px' }}>
                      {opt.description}
                    </Text>
                  </div>
                ))}
              </div>
            ) : (
              <Card elevation={Elevation.ZERO} style={{ 
                backgroundColor: '#30404D',
                padding: '16px',
                fontFamily: 'monospace',
                fontSize: '12px',
                color: '#A7B6C2'
              }}>
                {analysis.plan_analysis.optimized_plan}
              </Card>
            )}
          </Card>

          {/* Physical Plan */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              <Icon icon={IconNames.COG} style={{ marginRight: '8px' }} />
              Physical Execution Plan
            </H5>
            <Text style={{ color: '#A7B6C2', fontSize: '13px', marginBottom: '16px' }}>
              Actual execution strategy with cost estimates
            </Text>
            
            {Array.isArray(analysis.plan_analysis.physical_plan) ? (
              <HTMLTable 
                condensed 
                striped
                style={{ width: '100%', backgroundColor: '#30404D' }}
              >
                <thead>
                  <tr>
                    <th style={{ color: '#F5F8FA', padding: '8px' }}>Operation</th>
                    <th style={{ color: '#F5F8FA', padding: '8px' }}>Cost</th>
                    <th style={{ color: '#F5F8FA', padding: '8px' }}>Parallelism</th>
                  </tr>
                </thead>
                <tbody>
                  {analysis.plan_analysis.physical_plan.map((op, index) => (
                    <tr key={index}>
                      <td style={{ 
                        padding: '8px', 
                        fontFamily: 'monospace', 
                        fontSize: '12px',
                        color: '#F5F8FA'
                      }}>
                        {op.operation}
                      </td>
                      <td style={{ padding: '8px' }}>
                        <Tag 
                          minimal
                          style={{ 
                            backgroundColor: getCostColor(op.cost_estimate?.cost) + '22',
                            color: getCostColor(op.cost_estimate?.cost)
                          }}
                        >
                          {op.cost_estimate?.cost} ({op.cost_estimate?.type})
                        </Tag>
                      </td>
                      <td style={{ padding: '8px' }}>
                        <Tag 
                          minimal
                          intent={op.parallelism?.parallel ? Intent.SUCCESS : Intent.NONE}
                        >
                          {op.parallelism?.parallel ? 'Parallel' : 'Sequential'}
                        </Tag>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
            ) : (
              <Card elevation={Elevation.ZERO} style={{ 
                backgroundColor: '#30404D',
                padding: '16px',
                fontFamily: 'monospace',
                fontSize: '12px',
                color: '#A7B6C2'
              }}>
                {analysis.plan_analysis.physical_plan}
              </Card>
            )}
          </Card>
        </>
      )}
    </div>
  );

  return (
    <div style={{ padding: '32px', maxWidth: '1400px', margin: '0 auto' }}>
      <div style={{ marginBottom: '32px', textAlign: 'center' }}>
        <H3 style={{ marginBottom: '8px', color: '#F5F8FA' }}>Catalyst Optimizer Explorer</H3>
        <Text style={{ color: '#A7B6C2', fontSize: '16px' }}>
          Deep dive into Spark's Catalyst optimizer with execution plan analysis and rule applications
        </Text>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '32px' }}>
        {/* Query Input Panel */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          <Card elevation={Elevation.TWO} style={{ 
            backgroundColor: '#30404D',
            border: '1px solid #394B59',
            padding: '24px'
          }}>
            <H5 style={{ marginBottom: '20px', color: '#F5F8FA' }}>SQL Query</H5>
            
            <div style={{ marginBottom: '16px' }}>
              <Card elevation={Elevation.ONE} style={{ 
                border: '1px solid #5C7080',
                borderRadius: '6px',
                overflow: 'hidden'
              }}>
                <CodeMirror
                  value={query}
                  height="250px"
                  extensions={[sql()]}
                  theme={oneDark}
                  onChange={(value) => setQuery(value)}
                  placeholder="Enter your SQL query here..."
                />
              </Card>
            </div>

            <div style={{ marginBottom: '16px' }}>
              <Button
                icon={IconNames.DATABASE}
                onClick={generateSampleData}
                minimal={true}
                small={true}
              >
                Generate Sample Data ({selectedData.length} rows)
              </Button>
            </div>

            <Button
              intent={Intent.PRIMARY}
              fill={true}
              large={true}
              loading={loading}
              onClick={explainQuery}
              icon={IconNames.SEARCH}
              disabled={!query.trim()}
            >
              {loading ? 'Analyzing...' : 'Explain Query'}
            </Button>

            {error && (
              <Callout
                intent={Intent.DANGER}
                icon={IconNames.WARNING_SIGN}
                style={{ marginTop: '16px' }}
              >
                {error}
              </Callout>
            )}
          </Card>

          {/* Quick Info */}
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            border: '1px solid #5C7080',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
              What This Shows
            </H5>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Catalyst Rules:</strong> Which optimization rules were applied
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Execution Plans:</strong> Logical → Optimized → Physical
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Cost Analysis:</strong> Predicted performance bottlenecks
              </Text>
              <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                • <strong>Optimizations:</strong> Suggestions for query improvement
              </Text>
            </div>
          </Card>
        </div>

        {/* Analysis Results Panel */}
        <div>
          {analysis ? (
            <Tabs
              id="catalyst-analysis-tabs"
              selectedTabId={activeTab}
              onChange={(tabId) => setActiveTab(tabId as string)}
              large={true}
            >
              <Tab
                id="overview"
                title={
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Icon icon={IconNames.DASHBOARD} style={{ marginRight: '8px' }} />
                    Overview
                  </div>
                }
                panel={<OverviewPanel />}
              />
              <Tab
                id="plans"
                title={
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Icon icon={IconNames.DIAGRAM_TREE} style={{ marginRight: '8px' }} />
                    Execution Plans
                  </div>
                }
                panel={<ExecutionPlansPanel />}
              />
              <Tab
                id="raw"
                title={
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Icon icon={IconNames.CODE} style={{ marginRight: '8px' }} />
                    Raw Output
                  </div>
                }
                panel={
                  <Card elevation={Elevation.ONE} style={{ 
                    backgroundColor: '#394B59',
                    padding: '20px'
                  }}>
                    <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
                      Spark Explain Output
                    </H5>
                    <Card elevation={Elevation.ZERO} style={{ 
                      backgroundColor: '#30404D',
                      padding: '16px',
                      fontFamily: 'monospace',
                      fontSize: '11px',
                      color: '#A7B6C2',
                      whiteSpace: 'pre-wrap',
                      maxHeight: '500px',
                      overflow: 'auto'
                    }}>
                      {analysis.explain_output || 'No explain output available'}
                    </Card>
                  </Card>
                }
              />
            </Tabs>
          ) : (
            <Card elevation={Elevation.ONE} style={{ 
              backgroundColor: '#394B59',
              padding: '48px',
              textAlign: 'center'
            }}>
              <Icon 
                icon={IconNames.SEARCH} 
                size={48} 
                style={{ color: '#8A9BA8', marginBottom: '16px' }}
              />
              <H5 style={{ color: '#F5F8FA', marginBottom: '8px' }}>
                Ready to Analyze
              </H5>
              <Text style={{ color: '#A7B6C2' }}>
                Enter a SQL query and click "Explain Query" to see detailed Catalyst optimizer analysis
              </Text>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}