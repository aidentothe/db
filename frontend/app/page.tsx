'use client';

import { useState } from 'react';
import { Classes, Navbar, NavbarGroup, NavbarHeading, NavbarDivider, Text, H1, H5, Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import DatasetSelector from '../components/DatasetSelector';
import SQLQueryEditor from '../components/SQLQueryEditor';
import ComplexityDashboard from '../components/ComplexityDashboard';
import ResultsViewer from '../components/ResultsViewer';

export default function Home() {
  const [selectedData, setSelectedData] = useState<any[]>([]);
  const [dataStats, setDataStats] = useState<any>(null);
  const [queryResults, setQueryResults] = useState<any>(null);
  const [complexityAnalysis, setComplexityAnalysis] = useState<any>(null);

  const handleDataSelected = (data: any[], stats: any) => {
    setSelectedData(data);
    setDataStats(stats);
    setQueryResults(null);
    setComplexityAnalysis(null);
  };

  const handleQueryExecuted = (results: any, analysis: any) => {
    setQueryResults(results);
    setComplexityAnalysis(analysis);
  };

  return (
    <div className={Classes.DARK} style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Navbar className={Classes.DARK} style={{ flexShrink: 0 }}>
        <NavbarGroup>
          <NavbarHeading>
            <H1 style={{ margin: 0, fontSize: '24px' }}>SQL Query Complexity Analyzer</H1>
          </NavbarHeading>
          <NavbarDivider />
          <Text className={Classes.TEXT_MUTED}>
            Analyze memory and compute complexity of SQL queries on CSV data
          </Text>
        </NavbarGroup>
        <NavbarGroup align="right" className="navbar-group-right">
          <Button
            icon={IconNames.LIGHTNING}
            text="Spark Demo"
            minimal={true}
            onClick={() => window.location.href = '/spark-demo'}
            style={{ marginRight: '16px' }}
          />
          <Text className={Classes.TEXT_MUTED} style={{ fontSize: '12px', textAlign: 'right' }}>
            Powered by Next.js + Python<br/>
            Real-time complexity analysis
          </Text>
        </NavbarGroup>
      </Navbar>

      {/* Main Content */}
      <div className="main-content" style={{ 
        flex: 1, 
        padding: '24px', 
        backgroundColor: '#293742',
        display: 'flex',
        justifyContent: 'center'
      }}>
        <div className="main-grid" style={{ 
          display: 'grid', 
          gridTemplateColumns: 'minmax(320px, 1fr) minmax(600px, 2fr)', 
          gap: '24px', 
          maxWidth: '1600px', 
          width: '100%',
          alignItems: 'start'
        }}>
          {/* Left Side - Data Selection */}
          <div className="sidebar-sticky" style={{ display: 'flex', flexDirection: 'column', gap: '24px', position: 'sticky', top: '24px' }}>
            <DatasetSelector onDataSelected={handleDataSelected} />
            {complexityAnalysis && (
              <ComplexityDashboard analysis={complexityAnalysis} />
            )}
          </div>

          {/* Right Side - Query Editor and Results */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
            <SQLQueryEditor
              data={selectedData}
              dataStats={dataStats}
              onQueryExecuted={handleQueryExecuted}
            />
            {queryResults && <ResultsViewer results={queryResults} />}
          </div>
        </div>
      </div>

      {/* Footer */}
      <footer className={Classes.DARK} style={{ 
        flexShrink: 0,
        padding: '16px 24px', 
        textAlign: 'center', 
        borderTop: '1px solid #394B59',
        backgroundColor: '#30404D'
      }}>
        <Text className={Classes.TEXT_MUTED} style={{ fontSize: '12px' }}>
          SQL Query Complexity Analyzer - Instantly calculate memory and compute requirements<br/>
          Supporting CSV datasets with real-time complexity scoring
        </Text>
      </footer>
    </div>
  );
}