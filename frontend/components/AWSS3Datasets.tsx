'use client';

import { useState, useEffect } from 'react';
import {
  Card,
  Elevation,
  Icon,
  Button,
  Intent,
  Callout,
  Spinner,
  Text,
  H3,
  Tag,
  HTMLTable,
  NonIdealState
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';

export default function AWSS3Datasets() {
  const [isConnected, setIsConnected] = useState(false);
  const [isChecking, setIsChecking] = useState(true);
  const [datasets, setDatasets] = useState<any[]>([]);

  useEffect(() => {
    checkS3Connection();
  }, []);

  const checkS3Connection = async () => {
    setIsChecking(true);
    try {
      const response = await fetch('/api/spark/s3/status');
      const data = await response.json();
      setIsConnected(data.connected || false);
      if (data.connected && data.datasets) {
        setDatasets(data.datasets);
      }
    } catch (error) {
      setIsConnected(false);
    } finally {
      setIsChecking(false);
    }
  };

  if (isChecking) {
    return (
      <div style={{ padding: '32px', textAlign: 'center' }}>
        <Spinner size={50} />
        <Text style={{ marginTop: '16px', color: '#A7B6C2' }}>
          Checking AWS S3 connection...
        </Text>
      </div>
    );
  }

  if (!isConnected) {
    return (
      <div style={{ padding: '32px', maxWidth: '800px', margin: '0 auto' }}>
        <NonIdealState
          icon={IconNames.CLOUD_OFFLINE}
          title="AWS S3 Not Connected"
          description="To access S3 datasets, you need to configure AWS credentials and connection settings."
          action={
            <div style={{ marginTop: '24px' }}>
              <Callout intent={Intent.WARNING} icon={IconNames.WARNING_SIGN}>
                <H3>Connection Required</H3>
                <p>
                  AWS S3 connection is not configured. To enable S3 dataset access:
                </p>
                <ol style={{ marginLeft: '20px', marginTop: '12px' }}>
                  <li>Configure AWS credentials in your environment</li>
                  <li>Set up the S3 bucket permissions</li>
                  <li>Update the backend configuration with your S3 settings</li>
                </ol>
                <Button
                  intent={Intent.PRIMARY}
                  icon={IconNames.REFRESH}
                  text="Retry Connection"
                  onClick={checkS3Connection}
                  style={{ marginTop: '16px' }}
                />
              </Callout>
            </div>
          }
        />
      </div>
    );
  }

  return (
    <div style={{ padding: '32px' }}>
      <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
        <div style={{ display: 'flex', alignItems: 'center', marginBottom: '24px' }}>
          <Icon icon={IconNames.CLOUD} size={32} style={{ color: '#FF9933', marginRight: '16px' }} />
          <div>
            <H3 style={{ margin: 0 }}>AWS S3 Datasets</H3>
            <Text className="bp5-text-muted">Browse and analyze datasets stored in Amazon S3</Text>
          </div>
          <div style={{ marginLeft: 'auto' }}>
            <Tag intent={Intent.SUCCESS} icon={IconNames.TICK}>
              Connected
            </Tag>
          </div>
        </div>

        <Card elevation={Elevation.ONE} style={{ marginBottom: '24px' }}>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: '16px' }}>
            <Icon icon={IconNames.DATABASE} style={{ marginRight: '8px', color: '#48AFF0' }} />
            <Text style={{ fontWeight: 600 }}>Available Datasets</Text>
          </div>

          {datasets.length === 0 ? (
            <NonIdealState
              icon={IconNames.INBOX}
              title="No Datasets Found"
              description="No datasets were found in the configured S3 bucket."
            />
          ) : (
            <HTMLTable striped style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>Dataset Name</th>
                  <th>Size</th>
                  <th>Format</th>
                  <th>Last Modified</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {datasets.map((dataset, index) => (
                  <tr key={index}>
                    <td>
                      <Icon icon={IconNames.DOCUMENT} style={{ marginRight: '8px' }} />
                      {dataset.name}
                    </td>
                    <td>{dataset.size}</td>
                    <td>
                      <Tag minimal>{dataset.format}</Tag>
                    </td>
                    <td>{dataset.lastModified}</td>
                    <td>
                      <Button
                        small
                        minimal
                        icon={IconNames.EYE_OPEN}
                        text="Preview"
                        intent={Intent.PRIMARY}
                      />
                      <Button
                        small
                        minimal
                        icon={IconNames.DOWNLOAD}
                        text="Analyze"
                        style={{ marginLeft: '8px' }}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          )}
        </Card>

        <Card elevation={Elevation.ONE}>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: '16px' }}>
            <Icon icon={IconNames.INFO_SIGN} style={{ marginRight: '8px', color: '#48AFF0' }} />
            <Text style={{ fontWeight: 600 }}>S3 Configuration</Text>
          </div>
          <div style={{ 
            backgroundColor: '#30404D', 
            padding: '16px', 
            borderRadius: '4px',
            fontFamily: 'monospace',
            fontSize: '12px'
          }}>
            <div>Bucket: {process.env.NEXT_PUBLIC_S3_BUCKET || 'Not configured'}</div>
            <div>Region: {process.env.NEXT_PUBLIC_AWS_REGION || 'Not configured'}</div>
            <div>Prefix: {process.env.NEXT_PUBLIC_S3_PREFIX || '/'}</div>
          </div>
        </Card>
      </div>
    </div>
  );
}