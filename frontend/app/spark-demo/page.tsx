'use client';

import { useState } from 'react';
import { 
  Classes, 
  Navbar, 
  NavbarGroup, 
  NavbarHeading, 
  NavbarDivider, 
  Text, 
  H1, 
  Tab, 
  Tabs,
  Card,
  Elevation,
  Icon,
  Button,
  Intent
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';

// Import the new Spark demo components (will create these next)
import PerformanceBenchmark from '../../components/PerformanceBenchmark';
import LargeDatasetGenerator from '../../components/LargeDatasetGenerator';
import ETLPipelineViewer from '../../components/ETLPipelineViewer';
import StreamingDashboard from '../../components/StreamingDashboard';
import MLPipelineBuilder from '../../components/MLPipelineBuilder';
import SparkLearningCenter from '../../components/SparkLearningCenter';

export default function SparkDemo() {
  const [activeTab, setActiveTab] = useState<string>('overview');

  const sparkCapabilities = [
    {
      id: 'performance',
      title: 'Performance Comparison',
      description: 'See how Spark compares to Pandas across different data sizes and operations',
      icon: IconNames.TIMELINE_BAR_CHART,
      color: '#48AFF0'
    },
    {
      id: 'large-datasets',
      title: 'Large Dataset Generation',
      description: 'Generate and process millions of rows with distributed computing',
      icon: IconNames.DATABASE,
      color: '#29A634'
    },
    {
      id: 'etl',
      title: 'ETL Pipelines',
      description: 'Complex data transformations with data quality monitoring',
      icon: IconNames.FLOW_BRANCH,
      color: '#D9822B'
    },
    {
      id: 'streaming',
      title: 'Real-time Processing',
      description: 'Stream processing simulation with live analytics',
      icon: IconNames.TIMELINE_EVENTS,
      color: '#AD7BE9'
    },
    {
      id: 'ml',
      title: 'Machine Learning',
      description: 'End-to-end ML pipelines with automated feature engineering',
      icon: IconNames.PREDICTIVE_ANALYSIS,
      color: '#C23030'
    },
    {
      id: 'learning',
      title: 'Learning Center',
      description: 'Interactive tutorials and best practices for Apache Spark',
      icon: IconNames.LEARNING,
      color: '#0F9960'
    }
  ];

  const OverviewPanel = () => (
    <div style={{ padding: '32px', maxWidth: '1200px', margin: '0 auto' }}>
      {/* Hero Section */}
      <div style={{ textAlign: 'center', marginBottom: '48px' }}>
        <H1 style={{ 
          fontSize: '48px', 
          marginBottom: '16px',
          background: 'linear-gradient(135deg, #48AFF0 0%, #29A634 100%)',
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          fontWeight: 'bold'
        }}>
          Apache Spark Demo
        </H1>
        <Text style={{ 
          fontSize: '20px', 
          color: '#A7B6C2', 
          maxWidth: '600px', 
          margin: '0 auto',
          lineHeight: '1.6'
        }}>
          Explore the full power of Apache Spark through interactive demonstrations.
          From performance comparisons to machine learning pipelines.
        </Text>
      </div>

      {/* Capabilities Grid */}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(350px, 1fr))',
        gap: '24px',
        marginBottom: '48px'
      }}>
        {sparkCapabilities.map((capability) => (
          <Card 
            key={capability.id}
            elevation={Elevation.TWO}
            interactive={true}
            onClick={() => setActiveTab(capability.id)}
            style={{
              padding: '24px',
              backgroundColor: '#30404D',
              border: '1px solid #394B59',
              borderRadius: '12px',
              transition: 'all 0.3s ease',
              cursor: 'pointer',
              position: 'relative',
              overflow: 'hidden'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'translateY(-4px)';
              e.currentTarget.style.boxShadow = '0 12px 24px rgba(0,0,0,0.3)';
              e.currentTarget.style.borderColor = capability.color;
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'translateY(0)';
              e.currentTarget.style.boxShadow = '';
              e.currentTarget.style.borderColor = '#394B59';
            }}
          >
            {/* Gradient Background */}
            <div style={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              height: '4px',
              background: `linear-gradient(90deg, ${capability.color}, ${capability.color}88)`
            }} />
            
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '16px' }}>
              <Icon 
                icon={capability.icon} 
                size={32} 
                style={{ color: capability.color, flexShrink: 0, marginTop: '4px' }}
              />
              <div>
                <h3 style={{ 
                  margin: '0 0 8px 0', 
                  fontSize: '18px', 
                  fontWeight: '600',
                  color: '#F5F8FA'
                }}>
                  {capability.title}
                </h3>
                <Text style={{ 
                  color: '#A7B6C2', 
                  fontSize: '14px',
                  lineHeight: '1.5'
                }}>
                  {capability.description}
                </Text>
              </div>
            </div>
            
            <Button
              intent={Intent.PRIMARY}
              minimal={true}
              rightIcon={IconNames.ARROW_RIGHT}
              style={{ 
                marginTop: '16px',
                color: capability.color,
                border: `1px solid ${capability.color}44`
              }}
            >
              Explore
            </Button>
          </Card>
        ))}
      </div>

      {/* Key Features */}
      <Card elevation={Elevation.ONE} style={{ 
        backgroundColor: '#394B59',
        border: '1px solid #5C7080',
        borderRadius: '12px',
        padding: '32px'
      }}>
        <h2 style={{ 
          margin: '0 0 24px 0', 
          fontSize: '24px', 
          fontWeight: '600',
          color: '#F5F8FA',
          textAlign: 'center'
        }}>
          Why Apache Spark?
        </h2>
        
        <div style={{ 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
          gap: '24px'
        }}>
          {[
            {
              icon: IconNames.LIGHTNING,
              title: 'Lightning Fast',
              description: 'Up to 100x faster than Hadoop MapReduce for large-scale data processing'
            },
            {
              icon: IconNames.LAYERS,
              title: 'Unified Analytics',
              description: 'Single platform for batch processing, streaming, ML, and graph processing'
            },
            {
              icon: IconNames.CLOUD,
              title: 'Scalable',
              description: 'Scale from laptop to thousands of machines with fault tolerance'
            },
            {
              icon: IconNames.CODE,
              title: 'Developer Friendly',
              description: 'Rich APIs in Java, Scala, Python, R, and SQL with interactive shells'
            }
          ].map((feature, index) => (
            <div key={index} style={{ textAlign: 'center' }}>
              <Icon 
                icon={feature.icon} 
                size={24} 
                style={{ color: '#48AFF0', marginBottom: '12px' }}
              />
              <h4 style={{ 
                margin: '0 0 8px 0', 
                fontSize: '16px', 
                fontWeight: '600',
                color: '#F5F8FA'
              }}>
                {feature.title}
              </h4>
              <Text style={{ 
                color: '#A7B6C2', 
                fontSize: '13px',
                lineHeight: '1.4'
              }}>
                {feature.description}
              </Text>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );

  return (
    <div className={Classes.DARK} style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Navbar className={Classes.DARK} style={{ flexShrink: 0, backgroundColor: '#293742' }}>
        <NavbarGroup>
          <NavbarHeading>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <Icon icon={IconNames.LIGHTNING} size={24} style={{ marginRight: '8px', color: '#48AFF0' }} />
              <H1 style={{ margin: 0, fontSize: '20px', fontWeight: '600' }}>Spark Showcase</H1>
            </div>
          </NavbarHeading>
          <NavbarDivider />
          <Text className={Classes.TEXT_MUTED}>
            Interactive Apache Spark demonstrations and tutorials
          </Text>
        </NavbarGroup>
        <NavbarGroup align="right">
          <Button
            icon={IconNames.HOME}
            text="Back to SQL Demo"
            minimal={true}
            onClick={() => window.location.href = '/'}
          />
        </NavbarGroup>
      </Navbar>

      {/* Main Content */}
      <div style={{ 
        flex: 1, 
        backgroundColor: '#293742'
      }}>
        <Tabs
          id="spark-demo-tabs"
          selectedTabId={activeTab}
          onChange={(tabId) => setActiveTab(tabId as string)}
          large={true}
          style={{ 
            backgroundColor: '#30404D',
            borderBottom: '1px solid #394B59'
          }}
        >
          <Tab
            id="overview"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.DASHBOARD} style={{ marginRight: '8px' }} />
                Overview
              </div>
            }
            panel={<OverviewPanel />}
          />
          <Tab
            id="performance"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.TIMELINE_BAR_CHART} style={{ marginRight: '8px' }} />
                Performance
              </div>
            }
            panel={<PerformanceBenchmark />}
          />
          <Tab
            id="large-datasets"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.DATABASE} style={{ marginRight: '8px' }} />
                Large Datasets
              </div>
            }
            panel={<LargeDatasetGenerator />}
          />
          <Tab
            id="etl"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.FLOW_BRANCH} style={{ marginRight: '8px' }} />
                ETL Pipelines
              </div>
            }
            panel={<ETLPipelineViewer />}
          />
          <Tab
            id="streaming"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.TIMELINE_EVENTS} style={{ marginRight: '8px' }} />
                Streaming
              </div>
            }
            panel={<StreamingDashboard />}
          />
          <Tab
            id="ml"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.PREDICTIVE_ANALYSIS} style={{ marginRight: '8px' }} />
                Machine Learning
              </div>
            }
            panel={<MLPipelineBuilder />}
          />
          <Tab
            id="learning"
            title={
              <div style={{ display: 'flex', alignItems: 'center', padding: '8px 16px' }}>
                <Icon icon={IconNames.LEARNING} style={{ marginRight: '8px' }} />
                Learn
              </div>
            }
            panel={<SparkLearningCenter />}
          />
        </Tabs>
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
          Apache Spark Demo - Showcasing distributed computing, streaming, and machine learning capabilities
        </Text>
      </footer>
    </div>
  );
}