'use client';

import { Card, Elevation, H3, Text } from '@blueprintjs/core';

export default function StreamingDashboard() {
  return (
    <div style={{ padding: '32px', textAlign: 'center' }}>
      <Card elevation={Elevation.TWO} style={{ 
        backgroundColor: '#30404D',
        border: '1px solid #394B59',
        padding: '48px'
      }}>
        <H3 style={{ color: '#F5F8FA', marginBottom: '16px' }}>Streaming Dashboard</H3>
        <Text style={{ color: '#A7B6C2' }}>
          Coming soon: Real-time streaming data processing simulation with live analytics.
        </Text>
      </Card>
    </div>
  );
}