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
  HTMLTable,
  Icon,
  Divider,
  ProgressBar,
  FormGroup,
  InputGroup,
  NumericInput,
  HTMLSelect
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';

interface DeltaVersion {
  version: number;
  timestamp: string;
  operation: string;
  operation_parameters: any;
  user_metadata: any;
  is_blind_append: boolean;
  ready_commit_timestamp: number;
}

interface DeltaTableInfo {
  path: string;
  partitionColumns: string[];
  format: string;
  id: string;
  name: string;
  description: string;
  provider: string;
  properties: any;
}

interface TransactionResult {
  success: boolean;
  version: number;
  timestamp: string;
  rows_added: number;
  rows_updated: number;
  rows_deleted: number;
  transaction_id: string;
}

interface DeltaOperation {
  operation: string;
  sql: string;
  description: string;
  expected_outcome: string;
}

export default function DeltaLakeDemo() {
  const [selectedOperation, setSelectedOperation] = useState('create_table');
  const [customSQL, setCustomSQL] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');
  const [tableInfo, setTableInfo] = useState<DeltaTableInfo | null>(null);
  const [versionHistory, setVersionHistory] = useState<DeltaVersion[]>([]);
  const [transactionResults, setTransactionResults] = useState<TransactionResult[]>([]);
  const [activeTab, setActiveTab] = useState('operations');
  const [timeTravel, setTimeTravel] = useState({
    version: 0,
    timestamp: '',
    enabled: false
  });

  const deltaOperations: DeltaOperation[] = [
    {
      operation: 'create_table',
      sql: `CREATE TABLE IF NOT EXISTS delta_transactions (
  id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(10,2),
  category STRING,
  transaction_date TIMESTAMP,
  status STRING
) USING DELTA
PARTITIONED BY (category)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)`,
      description: 'Create a Delta table with partitioning and auto-optimization',
      expected_outcome: 'Creates optimized Delta table with ACID guarantees'
    },
    {
      operation: 'insert_data',
      sql: `INSERT INTO delta_transactions VALUES
(1, 101, 1250.50, 'electronics', current_timestamp(), 'completed'),
(2, 102, 75.25, 'books', current_timestamp(), 'completed'),
(3, 103, 299.99, 'clothing', current_timestamp(), 'pending'),
(4, 104, 450.00, 'electronics', current_timestamp(), 'completed'),
(5, 105, 125.75, 'books', current_timestamp(), 'failed')`,
      description: 'Insert sample transaction data',
      expected_outcome: 'Adds 5 transactions with ACID compliance'
    },
    {
      operation: 'upsert_merge',
      sql: `MERGE INTO delta_transactions AS target
USING (
  SELECT 3 as id, 103 as customer_id, 299.99 as amount, 
         'clothing' as category, current_timestamp() as transaction_date, 
         'completed' as status
  UNION ALL
  SELECT 6 as id, 106 as customer_id, 89.99 as amount,
         'home' as category, current_timestamp() as transaction_date,
         'completed' as status
) AS source
ON target.id = source.id
WHEN MATCHED THEN 
  UPDATE SET status = source.status
WHEN NOT MATCHED THEN 
  INSERT (id, customer_id, amount, category, transaction_date, status)
  VALUES (source.id, source.customer_id, source.amount, source.category, source.transaction_date, source.status)`,
      description: 'Demonstrate UPSERT with MERGE operation',
      expected_outcome: 'Updates existing record and inserts new record atomically'
    },
    {
      operation: 'delete_records',
      sql: `DELETE FROM delta_transactions 
WHERE status = 'failed' AND amount < 150.00`,
      description: 'Delete failed transactions under $150',
      expected_outcome: 'Removes failed transactions while maintaining consistency'
    },
    {
      operation: 'optimize_table',
      sql: `OPTIMIZE delta_transactions
ZORDER BY (customer_id, transaction_date)`,
      description: 'Optimize table with Z-Ordering for better query performance',
      expected_outcome: 'Compacts files and optimizes data layout'
    },
    {
      operation: 'vacuum_table',
      sql: `VACUUM delta_transactions RETAIN 168 HOURS`,
      description: 'Clean up old file versions (retain 7 days)',
      expected_outcome: 'Removes old data files while preserving time travel capability'
    }
  ];

  const executeOperation = async () => {
    if (!selectedOperation && !customSQL.trim()) {
      setError('Please select an operation or enter custom SQL');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const operation = deltaOperations.find(op => op.operation === selectedOperation);
      const sqlToExecute = customSQL.trim() || operation?.sql || '';

      const response = await fetch('/api/delta-operations', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          operation: selectedOperation,
          sql: sqlToExecute,
          time_travel: timeTravel.enabled ? timeTravel : null
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
        setTableInfo(result.table_info);
        setVersionHistory(result.version_history);
        if (result.transaction_result) {
          setTransactionResults(prev => [result.transaction_result, ...prev.slice(0, 9)]);
        }
      } else {
        setError(result.error);
      }
    } catch (err) {
      setError('Failed to execute operation: ' + (err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const getOperationIcon = (operation: string) => {
    switch (operation) {
      case 'CREATE TABLE': return IconNames.DATABASE;
      case 'INSERT': return IconNames.PLUS;
      case 'MERGE': return IconNames.MERGE_COLUMNS;
      case 'UPDATE': return IconNames.EDIT;
      case 'DELETE': return IconNames.TRASH;
      case 'OPTIMIZE': return IconNames.LIGHTNING;
      case 'VACUUM': return IconNames.CLEAN;
      default: return IconNames.DOCUMENT;
    }
  };

  const getOperationColor = (operation: string) => {
    switch (operation) {
      case 'CREATE TABLE': return '#29A634';
      case 'INSERT': return '#48AFF0';
      case 'MERGE': return '#D9822B';
      case 'UPDATE': return '#AD7FA8';
      case 'DELETE': return '#C23030';
      case 'OPTIMIZE': return '#FFB366';
      case 'VACUUM': return '#8A9BA8';
      default: return '#5C7080';
    }
  };

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toLocaleString();
  };

  const OperationsPanel = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {/* Operation Selection */}
      <Card elevation={Elevation.ONE} style={{ 
        backgroundColor: '#394B59',
        padding: '20px'
      }}>
        <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Delta Lake Operations</H5>
        
        <FormGroup label="Operation Type">
          <HTMLSelect
            value={selectedOperation}
            onChange={(e) => setSelectedOperation(e.currentTarget.value)}
            fill={true}
            options={deltaOperations.map(op => ({
              value: op.operation,
              label: op.description
            }))}
          />
        </FormGroup>

        {selectedOperation && (
          <div style={{ marginTop: '16px' }}>
            <Text style={{ color: '#A7B6C2', fontSize: '13px', marginBottom: '8px' }}>
              Expected Outcome:
            </Text>
            <Callout intent={Intent.PRIMARY} icon={IconNames.INFO_SIGN}>
              {deltaOperations.find(op => op.operation === selectedOperation)?.expected_outcome}
            </Callout>
          </div>
        )}
      </Card>

      {/* Custom SQL Editor */}
      <Card elevation={Elevation.ONE} style={{ 
        backgroundColor: '#394B59',
        padding: '20px'
      }}>
        <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Custom SQL (Optional)</H5>
        
        <Card elevation={Elevation.ONE} style={{ 
          border: '1px solid #5C7080',
          borderRadius: '6px',
          overflow: 'hidden',
          marginBottom: '16px'
        }}>
          <CodeMirror
            value={customSQL}
            height="150px"
            extensions={[sql()]}
            theme={oneDark}
            onChange={(value) => setCustomSQL(value)}
            placeholder="Enter custom Delta Lake SQL operations..."
          />
        </Card>

        <Text style={{ fontSize: '12px', color: '#8A9BA8' }}>
          Leave empty to use the predefined operation above
        </Text>
      </Card>

      {/* Time Travel Options */}
      <Card elevation={Elevation.ONE} style={{ 
        backgroundColor: '#394B59',
        padding: '20px'
      }}>
        <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Time Travel Query</H5>
        
        <FormGroup>
          <label style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <input
              type="checkbox"
              checked={timeTravel.enabled}
              onChange={(e) => setTimeTravel(prev => ({
                ...prev,
                enabled: e.target.checked
              }))}
            />
            <Text style={{ color: '#F5F8FA' }}>Enable Time Travel</Text>
          </label>
        </FormGroup>

        {timeTravel.enabled && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginTop: '12px' }}>
            <FormGroup label="Version">
              <NumericInput
                value={timeTravel.version}
                onValueChange={(value) => setTimeTravel(prev => ({
                  ...prev,
                  version: value
                }))}
                min={0}
                fill={true}
              />
            </FormGroup>
            <FormGroup label="Timestamp">
              <InputGroup
                value={timeTravel.timestamp}
                onChange={(e) => setTimeTravel(prev => ({
                  ...prev,
                  timestamp: e.target.value
                }))}
                placeholder="YYYY-MM-DD HH:MM:SS"
                fill={true}
              />
            </FormGroup>
          </div>
        )}
      </Card>

      {/* Execute Button */}
      <Button
        intent={Intent.PRIMARY}
        fill={true}
        large={true}
        loading={loading}
        onClick={executeOperation}
        icon={IconNames.PLAY}
      >
        {loading ? 'Executing...' : 'Execute Operation'}
      </Button>

      {error && (
        <Callout
          intent={Intent.DANGER}
          icon={IconNames.WARNING_SIGN}
        >
          {error}
        </Callout>
      )}
    </div>
  );

  const VersionHistoryPanel = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {versionHistory.length > 0 ? (
        <>
          <Card elevation={Elevation.ONE} style={{ 
            backgroundColor: '#394B59',
            padding: '20px'
          }}>
            <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Version Timeline</H5>
            
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              {versionHistory.slice(0, 10).map((version, index) => (
                <div
                  key={version.version}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '12px',
                    backgroundColor: index === 0 ? '#29A63422' : '#30404D',
                    borderRadius: '6px',
                    border: `1px solid ${index === 0 ? '#29A634' : '#5C7080'}`
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <Icon 
                      icon={getOperationIcon(version.operation)} 
                      size={16}
                      style={{ color: getOperationColor(version.operation) }}
                    />
                    <div>
                      <Text style={{ 
                        fontWeight: 'bold', 
                        color: '#F5F8FA',
                        marginBottom: '2px'
                      }}>
                        Version {version.version}
                      </Text>
                      <Text style={{ 
                        fontSize: '12px', 
                        color: '#A7B6C2',
                        display: 'block'
                      }}>
                        {formatTimestamp(version.timestamp)}
                      </Text>
                    </div>
                  </div>
                  
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Tag 
                      style={{ 
                        backgroundColor: getOperationColor(version.operation) + '22',
                        color: getOperationColor(version.operation)
                      }}
                    >
                      {version.operation}
                    </Tag>
                    {index === 0 && (
                      <Tag intent={Intent.SUCCESS} minimal>
                        Current
                      </Tag>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </Card>

          {/* Table Information */}
          {tableInfo && (
            <Card elevation={Elevation.ONE} style={{ 
              backgroundColor: '#394B59',
              padding: '20px'
            }}>
              <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Table Information</H5>
              
              <HTMLTable condensed striped style={{ width: '100%' }}>
                <tbody>
                  <tr>
                    <td style={{ color: '#A7B6C2', fontWeight: 'bold' }}>Table ID</td>
                    <td style={{ color: '#F5F8FA', fontFamily: 'monospace' }}>{tableInfo.id}</td>
                  </tr>
                  <tr>
                    <td style={{ color: '#A7B6C2', fontWeight: 'bold' }}>Format</td>
                    <td style={{ color: '#F5F8FA' }}>{tableInfo.format}</td>
                  </tr>
                  <tr>
                    <td style={{ color: '#A7B6C2', fontWeight: 'bold' }}>Partition Columns</td>
                    <td style={{ color: '#F5F8FA' }}>
                      {tableInfo.partitionColumns.length > 0 
                        ? tableInfo.partitionColumns.join(', ')
                        : 'None'
                      }
                    </td>
                  </tr>
                  <tr>
                    <td style={{ color: '#A7B6C2', fontWeight: 'bold' }}>Provider</td>
                    <td style={{ color: '#F5F8FA' }}>{tableInfo.provider}</td>
                  </tr>
                </tbody>
              </HTMLTable>
            </Card>
          )}
        </>
      ) : (
        <Card elevation={Elevation.ONE} style={{ 
          backgroundColor: '#394B59',
          padding: '48px',
          textAlign: 'center'
        }}>
          <Icon 
            icon={IconNames.HISTORY} 
            size={48} 
            style={{ color: '#8A9BA8', marginBottom: '16px' }}
          />
          <H5 style={{ color: '#F5F8FA', marginBottom: '8px' }}>
            No Version History
          </H5>
          <Text style={{ color: '#A7B6C2' }}>
            Execute some Delta operations to see version history and time travel capabilities
          </Text>
        </Card>
      )}
    </div>
  );

  const TransactionsPanel = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
      {transactionResults.length > 0 ? (
        <Card elevation={Elevation.ONE} style={{ 
          backgroundColor: '#394B59',
          padding: '20px'
        }}>
          <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>Recent Transactions</H5>
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {transactionResults.map((transaction, index) => (
              <div
                key={transaction.transaction_id}
                style={{
                  padding: '16px',
                  backgroundColor: '#30404D',
                  borderRadius: '6px',
                  border: '1px solid #5C7080'
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                  <Text style={{ fontWeight: 'bold', color: '#F5F8FA' }}>
                    Transaction {transaction.transaction_id}
                  </Text>
                  <Text style={{ fontSize: '12px', color: '#A7B6C2' }}>
                    v{transaction.version} • {formatTimestamp(transaction.timestamp)}
                  </Text>
                </div>
                
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '12px' }}>
                  <div style={{ textAlign: 'center' }}>
                    <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>ADDED</Text>
                    <Text style={{ fontSize: '18px', fontWeight: 'bold', color: '#29A634' }}>
                      {transaction.rows_added}
                    </Text>
                  </div>
                  <div style={{ textAlign: 'center' }}>
                    <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>UPDATED</Text>
                    <Text style={{ fontSize: '18px', fontWeight: 'bold', color: '#48AFF0' }}>
                      {transaction.rows_updated}
                    </Text>
                  </div>
                  <div style={{ textAlign: 'center' }}>
                    <Text style={{ color: '#A7B6C2', fontSize: '12px' }}>DELETED</Text>
                    <Text style={{ fontSize: '18px', fontWeight: 'bold', color: '#C23030' }}>
                      {transaction.rows_deleted}
                    </Text>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Card>
      ) : (
        <Card elevation={Elevation.ONE} style={{ 
          backgroundColor: '#394B59',
          padding: '48px',
          textAlign: 'center'
        }}>
          <Icon 
            icon={IconNames.EXCHANGE} 
            size={48} 
            style={{ color: '#8A9BA8', marginBottom: '16px' }}
          />
          <H5 style={{ color: '#F5F8FA', marginBottom: '8px' }}>
            No Transactions Yet
          </H5>
          <Text style={{ color: '#A7B6C2' }}>
            Execute Delta operations to see ACID transaction details and metrics
          </Text>
        </Card>
      )}
    </div>
  );

  return (
    <div style={{ padding: '32px', maxWidth: '1400px', margin: '0 auto' }}>
      <div style={{ marginBottom: '32px', textAlign: 'center' }}>
        <H3 style={{ marginBottom: '8px', color: '#F5F8FA' }}>Delta Lake Operations</H3>
        <Text style={{ color: '#A7B6C2', fontSize: '16px' }}>
          ACID transactions, time travel, and schema evolution with Delta Lake
        </Text>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '32px' }}>
        {/* Operations Panel */}
        <div>
          <OperationsPanel />
        </div>

        {/* Results Panel */}
        <div>
          <Tabs
            id="delta-demo-tabs"
            selectedTabId={activeTab}
            onChange={(tabId) => setActiveTab(tabId as string)}
            large={true}
          >
            <Tab
              id="operations"
              title={
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  <Icon icon={IconNames.COG} style={{ marginRight: '8px' }} />
                  Operations
                </div>
              }
              panel={
                <Card elevation={Elevation.ONE} style={{ 
                  backgroundColor: '#394B59',
                  padding: '20px'
                }}>
                  <H5 style={{ marginBottom: '16px', color: '#F5F8FA' }}>
                    Delta Lake Features
                  </H5>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      • <strong>ACID Transactions:</strong> Guaranteed consistency across operations
                    </Text>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      • <strong>Time Travel:</strong> Query data at any point in time
                    </Text>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      • <strong>Schema Evolution:</strong> Safely evolve table schema over time
                    </Text>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      • <strong>UPSERT Operations:</strong> Efficient merge and update patterns
                    </Text>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      • <strong>Optimizations:</strong> Z-ordering and file compaction
                    </Text>
                    <Text style={{ fontSize: '13px', color: '#A7B6C2' }}>
                      • <strong>Data Versioning:</strong> Complete audit trail of all changes
                    </Text>
                  </div>
                </Card>
              }
            />
            <Tab
              id="history"
              title={
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  <Icon icon={IconNames.HISTORY} style={{ marginRight: '8px' }} />
                  Version History
                </div>
              }
              panel={<VersionHistoryPanel />}
            />
            <Tab
              id="transactions"
              title={
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  <Icon icon={IconNames.EXCHANGE} style={{ marginRight: '8px' }} />
                  Transactions
                </div>
              }
              panel={<TransactionsPanel />}
            />
          </Tabs>
        </div>
      </div>
    </div>
  );
}