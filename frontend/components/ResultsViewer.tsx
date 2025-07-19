'use client';

import { useState, useMemo } from 'react';
import { 
  Button, 
  Card, 
  Elevation, 
  H3, 
  H5, 
  Text, 
  Tab, 
  Tabs, 
  Intent, 
  Classes,
  Tag,
  ButtonGroup,
  Icon
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Table2, Column, Cell, TableLoadingOption } from '@blueprintjs/table';

interface ResultsViewerProps {
  results: any;
}

export default function ResultsViewer({ results }: ResultsViewerProps) {
  const [viewMode, setViewMode] = useState<'table' | 'json'>('table');

  const { data, columns } = useMemo(() => {
    if (!results || !results.results) {
      return { data: [], columns: [] };
    }

    // Handle array of objects with CSV-like structure
    if (Array.isArray(results.results) && results.results.length > 0) {
      const firstItem = results.results[0];
      const keys = Object.keys(firstItem);
      
      // Check if this is the CSV format where header is the key and row is the value
      if (keys.length === 1 && keys[0].includes(',')) {
        const headerKey = keys[0];
        
        // Parse headers from the key
        const headers = headerKey.split(',').map(h => h.trim());
        
        // Parse each row
        const rows = results.results.map((item: any) => {
          const rowValue = item[headerKey];
          
          // Simple CSV parsing - split by comma but handle quoted values
          const values: string[] = [];
          let current = '';
          let inQuotes = false;
          
          for (let i = 0; i < rowValue.length; i++) {
            const char = rowValue[i];
            if (char === '"') {
              inQuotes = !inQuotes;
              current += char;
            } else if (char === ',' && !inQuotes) {
              values.push(current.trim());
              current = '';
            } else {
              current += char;
            }
          }
          values.push(current.trim());
          
          const row: any = {};
          headers.forEach((header, headerIndex) => {
            row[header] = values[headerIndex] || '';
          });
          
          return row;
        });
        
        return { data: rows, columns: headers };
      }
      
      // Standard object array format
      return { data: results.results, columns: keys };
    }

    return { data: [], columns: [] };
  }, [results]);


  if (!results || (!data || data.length === 0)) {
    return (
      <Card elevation={Elevation.TWO} style={{ 
        padding: '48px', 
        textAlign: 'center',
        backgroundColor: '#394B59',
        border: '1px solid #5C7080',
        borderRadius: '8px'
      }}>
        <Icon icon={IconNames.TABLE} size={48} style={{ 
          marginBottom: '20px',
          color: '#8A9BA8'
        }} />
        <H5 style={{ 
          margin: '0 0 8px 0',
          color: '#A7B6C2',
          fontSize: '16px',
          fontWeight: '500'
        }}>
          No results to display
        </H5>
        <Text className={Classes.TEXT_MUTED} style={{ fontSize: '13px' }}>
          Execute a query to see results here
        </Text>
      </Card>
    );
  }


  const downloadCSV = () => {
    if (!data.length) return;

    const csvContent = [
      columns.join(','),
      ...data.map((row: any) => 
        columns.map(col => {
          const value = row[col];
          // Escape quotes and wrap in quotes if contains comma
          if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
            return `"${value.replace(/"/g, '""')}"`;
          }
          return value;
        }).join(',')
      )
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'query_results.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  };

  const getColumnType = (columnName: string) => {
    if (!data.length) return 'text';
    const sampleValue = data[0][columnName];
    if (typeof sampleValue === 'number') return 'number';
    if (typeof sampleValue === 'boolean') return 'boolean';
    return 'text';
  };

  const formatValue = (value: any) => {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'number') {
      return Number.isInteger(value) ? value.toString() : value.toFixed(2);
    }
    if (typeof value === 'boolean') return value.toString();
    return String(value);
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'number': return '123';
      case 'boolean': return 'T/F';
      default: return 'Aa';
    }
  };

  const getTypeIntent = (type: string): Intent => {
    switch (type) {
      case 'number': return Intent.PRIMARY;
      case 'boolean': return Intent.SUCCESS;
      default: return Intent.NONE;
    }
  };

  return (
    <Card elevation={Elevation.TWO} style={{ 
      backgroundColor: '#394B59',
      border: '1px solid #5C7080',
      borderRadius: '8px'
    }}>
      <div style={{ padding: '24px' }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
          <div>
            <H3 style={{ 
              display: 'flex', 
              alignItems: 'center', 
              margin: 0,
              color: '#F5F8FA',
              fontSize: '18px',
              fontWeight: '600'
            }}>
              <Icon icon={IconNames.CHART} size={20} style={{ marginRight: '12px' }} />
              Query Results
            </H3>
            <Text className={Classes.TEXT_MUTED} style={{ marginTop: '4px', fontSize: '13px' }}>
              {results.row_count} rows returned
              {results.complexity_analysis && (
                <span> • Complexity: {results.complexity_analysis.complexity_rating}/10</span>
              )}
            </Text>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <ButtonGroup>
              <Button
                icon={IconNames.TABLE}
                onClick={() => setViewMode('table')}
                active={viewMode === 'table'}
                text="Table"
              />
              <Button
                icon={IconNames.EYE_OPEN}
                onClick={() => setViewMode('json')}
                active={viewMode === 'json'}
                text="JSON"
              />
            </ButtonGroup>
            <Button
              icon={IconNames.DOWNLOAD}
              onClick={downloadCSV}
              intent={Intent.SUCCESS}
              text="CSV"
            />
          </div>
        </div>

        {/* Column Types */}
        {viewMode === 'table' && (
          <div style={{ marginBottom: '20px' }}>
            <H5 style={{ marginBottom: '10px' }}>Column Types:</H5>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
              {columns.map(col => {
                const type = getColumnType(col);
                return (
                  <Tag
                    key={col}
                    intent={getTypeIntent(type)}
                    minimal={true}
                  >
                    {getTypeIcon(type)} {col}
                  </Tag>
                );
              })}
            </div>
          </div>
        )}

        {/* Content */}
        {viewMode === 'table' ? (
          <div style={{ 
            marginBottom: '24px',
            borderRadius: '6px',
            border: '1px solid #5C7080',
            overflow: 'hidden',
            height: '500px'
          }}>
            <Table2
              numRows={data.length}
              enableRowHeader={false}
              enableColumnHeader={true}
              enableMultipleSelection={false}
              enableGhostCells={false}
              enableFocusedCell={true}
            >
              {columns.map((column, index) => (
                <Column
                  key={index}
                  name={column}
                  cellRenderer={(rowIndex: number) => (
                    <Cell>
                      {data[rowIndex] ? formatValue(data[rowIndex][column]) : ''}
                    </Cell>
                  )}
                />
              ))}
            </Table2>
          </div>
        ) : (
          <Card elevation={Elevation.ONE} style={{ 
            padding: '18px', 
            marginBottom: '24px', 
            maxHeight: '450px', 
            overflow: 'auto',
            backgroundColor: '#30404D',
            border: '1px solid #5C7080',
            borderRadius: '6px'
          }}>
            <div style={{ marginBottom: '10px', fontSize: '11px', color: '#8A9BA8' }}>
              Parsed Data ({data.length} rows, {columns.length} columns)
            </div>
            <pre className={Classes.CODE_BLOCK} style={{ 
              color: '#CED9E0',
              fontSize: '12px',
              lineHeight: '1.4',
              margin: 0,
              backgroundColor: 'transparent'
            }}>
              {JSON.stringify(data, null, 2)}
            </pre>
          </Card>
        )}

        {/* Stats Footer */}
        <div style={{ paddingTop: '20px', borderTop: '1px solid #5C7080' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Text className={Classes.TEXT_MUTED} style={{ fontSize: '12px' }}>
              Showing {Math.min(data.length, 1000)} of {results.row_count || data.length} rows
              {data.length >= 1000 && (
                <span style={{ color: '#FF7373', fontWeight: '500' }}> (limited to 1000 for display)</span>
              )}
            </Text>
            <Text className={Classes.TEXT_MUTED} style={{ fontSize: '12px' }}>
              {columns.length} columns • {data.length} rows displayed
            </Text>
          </div>
        </div>
      </div>
    </Card>
  );
}