'use client';

import { useState, useEffect, useCallback } from 'react';
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
  FileInput,
  Callout,
  Spinner,
  NonIdealState,
  Icon
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { logger } from '../utils/logger';

interface Dataset {
  filename: string;
  name: string;
  rows: number;
  columns: number;
  complexity: string;
  size_bytes: number;
  preview: any[];
  column_types?: any;
}

interface DatasetSelectorProps {
  onDataSelected: (data: any[], stats: any) => void;
}

export default function DatasetSelector({ onDataSelected }: DatasetSelectorProps) {
  const [sampleDatasets, setSampleDatasets] = useState<Dataset[]>([]);
  const [uploadedFiles, setUploadedFiles] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(false);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<string>('');
  const [uploadError, setUploadError] = useState<string>('');
  const [activeTab, setActiveTab] = useState<'sample' | 'upload' | 's3'>('sample');
  const [dragOver, setDragOver] = useState(false);
  const [s3Connected, setS3Connected] = useState(false);
  const [s3Checking, setS3Checking] = useState(false);

  // Load sample datasets
  useEffect(() => {
    logger.info('DatasetSelector component initialized', {
      operation: 'component_init',
      component: 'DatasetSelector',
      user_action: 'component_mount'
    });
    
    loadSampleDatasets();
    loadUploadedFiles();
    checkS3Connection();
  }, []);

  const loadSampleDatasets = async () => {
    const timer = logger.startTimer();
    logger.logApiRequest('/api/sample-datasets', 'GET');
    
    try {
      const response = await fetch('/api/sample-datasets');
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Server returned non-JSON response. Make sure the backend is running.');
      }

      const result = await response.json();
      const processingTime = timer();
      
      if (result.success) {
        setSampleDatasets(result.datasets);
        logger.logApiSuccess('/api/sample-datasets', 'GET', processingTime);
        logger.info('Sample datasets loaded successfully', {
          operation: 'sample_datasets_load_success',
          component: 'DatasetSelector',
          datasets_count: result.datasets.length,
          processing_time: processingTime
        });
      } else {
        logger.warn('Sample datasets load returned error', {
          operation: 'sample_datasets_load_warning',
          component: 'DatasetSelector',
          error_message: result.error,
          processing_time: processingTime
        });
      }
    } catch (error) {
      const processingTime = timer();
      logger.logApiError('/api/sample-datasets', 'GET', error, processingTime);
    }
  };

  const loadUploadedFiles = async () => {
    const timer = logger.startTimer();
    logger.logApiRequest('/api/uploaded-files', 'GET');
    
    try {
      const response = await fetch('/api/uploaded-files');
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Server returned non-JSON response. Make sure the backend is running.');
      }

      const result = await response.json();
      const processingTime = timer();
      
      if (result.success) {
        setUploadedFiles(result.files);
        logger.logApiSuccess('/api/uploaded-files', 'GET', processingTime);
        logger.info('Uploaded files loaded successfully', {
          operation: 'uploaded_files_load_success',
          component: 'DatasetSelector',
          files_count: result.files.length,
          processing_time: processingTime
        });
      } else {
        logger.warn('Uploaded files load returned error', {
          operation: 'uploaded_files_load_warning',
          component: 'DatasetSelector',
          error_message: result.error,
          processing_time: processingTime
        });
      }
    } catch (error) {
      const processingTime = timer();
      logger.logApiError('/api/uploaded-files', 'GET', error, processingTime);
    }
  };

  const loadDataset = async (filename: string, isUploaded: boolean = false) => {
    setLoading(true);
    const timer = logger.startTimer();
    logger.logDatasetLoadStart(filename, isUploaded);
    
    try {
      const endpoint = isUploaded 
        ? `/api/load-uploaded-file/${filename}`
        : `/api/load-sample-dataset/${filename}`;
      
      logger.logApiRequest(endpoint, 'GET');
      const response = await fetch(endpoint);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Server returned non-JSON response. Make sure the backend is running.');
      }

      const result = await response.json();
      const processingTime = timer();
      
      if (result.success) {
        onDataSelected(result.data, result.full_stats || result.complexity_stats);
        logger.logDatasetLoadSuccess(filename, result.data.length, processingTime);
        logger.logApiSuccess(endpoint, 'GET', processingTime);
      } else {
        logger.warn('Dataset load returned error', {
          operation: 'dataset_load_warning',
          component: 'DatasetSelector',
          dataset_name: filename,
          is_uploaded: isUploaded,
          error_message: result.error,
          processing_time: processingTime
        });
        alert('Error loading dataset: ' + result.error);
      }
    } catch (error) {
      const processingTime = timer();
      logger.logDatasetLoadError(filename, error, processingTime);
      logger.logApiError(`/api/load-${isUploaded ? 'uploaded' : 'sample'}-file/${filename}`, 'GET', error, processingTime);
      alert('Error loading dataset');
    } finally {
      setLoading(false);
    }
  };

  const handleFileUpload = async (file: File, retryCount: number = 0) => {
    const MAX_RETRIES = 3;
    const RETRY_DELAY = 1000; // 1 second
    
    setUploadLoading(true);
    setUploadProgress('Validating file...');
    setUploadError('');
    const timer = logger.startTimer();
    logger.logFileUploadStart(file.name, file.size);
    
    // Validate file before upload
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setUploadError('Please select a CSV file');
      setUploadLoading(false);
      setUploadProgress('');
      return;
    }
    
    if (file.size > 50 * 1024 * 1024) { // 50MB limit
      setUploadError('File size too large. Please select a file smaller than 50MB');
      setUploadLoading(false);
      setUploadProgress('');
      return;
    }

    setUploadProgress('Uploading file...');
    const formData = new FormData();
    formData.append('file', file);

    try {
      logger.logApiRequest('/api/upload-csv', 'POST');
      const response = await fetch('/api/upload-csv', {
        method: 'POST',
        body: formData,
        signal: AbortSignal.timeout(120000), // 2 minute timeout
      });

      const processingTime = timer();
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Server returned non-JSON response. Make sure the backend is running.');
      }

      setUploadProgress('Processing file...');
      const result = await response.json();
      
      if (result.success) {
        logger.logFileUploadSuccess(file.name, file.size, processingTime);
        logger.logApiSuccess('/api/upload-csv', 'POST', processingTime);
        
        setUploadProgress('Refreshing file list...');
        await loadUploadedFiles(); // Refresh the list
        setActiveTab('upload'); // Switch to upload tab
        setUploadError('');
        alert(result.message);
      } else {
        logger.warn('File upload returned error', {
          operation: 'file_upload_warning',
          component: 'DatasetSelector',
          file_name: file.name,
          file_size: file.size,
          error_message: result.error,
          processing_time: processingTime,
          retry_count: retryCount
        });
        
        // Retry for server errors but not for client errors
        if (retryCount < MAX_RETRIES && result.error && 
            !result.error.toLowerCase().includes('format') && 
            !result.error.toLowerCase().includes('invalid')) {
          logger.info('Retrying file upload', {
            operation: 'file_upload_retry',
            component: 'DatasetSelector',
            file_name: file.name,
            retry_count: retryCount + 1,
            max_retries: MAX_RETRIES
          });
          
          setUploadProgress(`Retrying upload (${retryCount + 1}/${MAX_RETRIES})...`);
          await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
          return handleFileUpload(file, retryCount + 1);
        }
        
        setUploadError('Upload failed: ' + result.error);
      }
    } catch (error) {
      const processingTime = timer();
      const errorObj = error instanceof Error ? error : new Error(String(error || 'Unknown upload error'));
      logger.logFileUploadError(file.name, errorObj, processingTime);
      logger.logApiError('/api/upload-csv', 'POST', errorObj, processingTime);
      
      // Retry for network errors
      if (retryCount < MAX_RETRIES && (
          error instanceof TypeError || // Network errors
          (error as Error).message.includes('fetch') ||
          (error as Error).message.includes('timeout') ||
          (error as Error).message.includes('HTTP 5')
        )) {
        logger.info('Retrying file upload after error', {
          operation: 'file_upload_retry_after_error',
          component: 'DatasetSelector',
          file_name: file.name,
          retry_count: retryCount + 1,
          max_retries: MAX_RETRIES,
          error_message: (error as Error).message
        });
        
        setUploadProgress(`Retrying upload (${retryCount + 1}/${MAX_RETRIES})...`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
        return handleFileUpload(file, retryCount + 1);
      }
      
      setUploadError('Upload failed: ' + (error as Error).message);
    } finally {
      setUploadLoading(false);
      setUploadProgress('');
    }
  };

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    
    const files = Array.from(e.dataTransfer.files);
    const csvFiles = files.filter(file => file.name.toLowerCase().endsWith('.csv'));
    
    logger.info('Files dropped in upload area', {
      operation: 'files_dropped',
      component: 'DatasetSelector',
      total_files: files.length,
      csv_files: csvFiles.length,
      file_names: files.map(f => f.name),
      user_action: 'drag_drop'
    });
    
    if (csvFiles.length > 0) {
      logger.info('Processing dropped CSV file', {
        operation: 'process_dropped_csv',
        component: 'DatasetSelector',
        file_name: csvFiles[0].name,
        file_size: csvFiles[0].size,
        user_action: 'drag_drop'
      });
      handleFileUpload(csvFiles[0]); // Handle first CSV file
    } else {
      logger.warn('No CSV files in drop', {
        operation: 'invalid_drop',
        component: 'DatasetSelector',
        file_types: files.map(f => f.type),
        user_action: 'drag_drop'
      });
      alert('Please drop a CSV file');
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
  }, []);

  const checkS3Connection = async () => {
    setS3Checking(true);
    try {
      const response = await fetch('/api/s3/status');
      const data = await response.json();
      setS3Connected(data.connected || false);
    } catch (error) {
      setS3Connected(false);
    } finally {
      setS3Checking(false);
    }
  };

  const deleteUploadedFile = async (filename: string) => {
    if (!confirm('Are you sure you want to delete this file?')) return;

    const timer = logger.startTimer();
    logger.info('File deletion started', {
      operation: 'file_delete_start',
      component: 'DatasetSelector',
      file_name: filename,
      user_action: 'file_delete'
    });

    try {
      const endpoint = `/api/delete-uploaded-file/${filename}`;
      logger.logApiRequest(endpoint, 'DELETE');
      
      const response = await fetch(endpoint, {
        method: 'DELETE',
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Server returned non-JSON response. Make sure the backend is running.');
      }

      const result = await response.json();
      const processingTime = timer();
      
      if (result.success) {
        logger.info('File deleted successfully', {
          operation: 'file_delete_success',
          component: 'DatasetSelector',
          file_name: filename,
          processing_time: processingTime,
          user_action: 'file_delete'
        });
        logger.logApiSuccess(endpoint, 'DELETE', processingTime);
        
        await loadUploadedFiles(); // Refresh the list
      } else {
        logger.warn('File deletion returned error', {
          operation: 'file_delete_warning',
          component: 'DatasetSelector',
          file_name: filename,
          error_message: result.error,
          processing_time: processingTime
        });
        alert('Delete failed: ' + result.error);
      }
    } catch (error) {
      const processingTime = timer();
      logger.error('File deletion failed', {
        operation: 'file_delete_failed',
        component: 'DatasetSelector',
        file_name: filename,
        error_type: (error as Error).name || 'unknown',
        error_message: (error as Error).message || String(error),
        processing_time: processingTime,
        user_action: 'file_delete'
      });
      logger.logApiError(`/api/delete-uploaded-file/${filename}`, 'DELETE', error, processingTime);
      alert('Delete failed');
    }
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const getComplexityIntent = (complexity: string): Intent => {
    switch (complexity.toLowerCase()) {
      case 'simple': return Intent.SUCCESS;
      case 'medium': return Intent.WARNING;
      case 'complex': return Intent.DANGER;
      case 'very complex': return Intent.DANGER;
      default: return Intent.NONE;
    }
  };

  const DatasetCard = ({ dataset, isUploaded = false }: { dataset: Dataset; isUploaded?: boolean }) => (
    <Card elevation={Elevation.ONE} interactive={true} style={{ 
      padding: '18px',
      backgroundColor: '#30404D',
      border: '1px solid #394B59',
      borderRadius: '8px',
      transition: 'all 0.2s ease'
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', marginBottom: '12px' }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <H5 style={{ margin: 0, color: '#F5F8FA', fontSize: '16px', fontWeight: '600' }}>
            {dataset.name}
          </H5>
          <Text className={Classes.TEXT_MUTED} style={{ fontSize: '13px', marginTop: '4px' }}>
            {dataset.rows.toLocaleString()} rows × {dataset.columns} columns
          </Text>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
          <Tag 
            intent={getComplexityIntent(dataset.complexity)}
            style={{ fontSize: '11px', fontWeight: '500' }}
          >
            {dataset.complexity}
          </Tag>
          {isUploaded && (
            <Button
              icon={IconNames.TRASH}
              onClick={() => deleteUploadedFile(dataset.filename)}
              intent={Intent.DANGER}
              minimal={true}
              title="Delete file"
              style={{ padding: '4px' }}
            />
          )}
        </div>
      </div>
      
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
        <Text className={Classes.TEXT_MUTED} style={{ fontSize: '12px' }}>
          Size: {formatFileSize(dataset.size_bytes)}
        </Text>
        <Text className={Classes.TEXT_MUTED} style={{ fontSize: '11px' }}>
          {dataset.filename}
        </Text>
      </div>

      {/* Preview */}
      {dataset.preview && dataset.preview.length > 0 && (
        <div style={{ marginBottom: '15px' }}>
          <Text className={Classes.TEXT_SMALL} style={{ marginBottom: '8px', fontWeight: 'bold', color: '#5C7080' }}>
            Data Preview:
          </Text>
          <Card elevation={Elevation.ZERO} style={{ 
            padding: '12px', 
            backgroundColor: '#394B59',
            border: '1px solid #5C7080',
            borderRadius: '6px',
            overflow: 'hidden'
          }}>
            <div style={{ 
              overflowX: 'auto',
              width: '100%'
            }}>
              <div className="dataset-preview-grid" style={{ 
                display: 'grid', 
                gridTemplateColumns: `repeat(${Math.min(Object.keys(dataset.preview[0]).length, 4)}, minmax(60px, 1fr))`,
                gap: '4px',
                fontSize: '10px',
                minWidth: 'fit-content',
                maxWidth: '100%'
              }}>
                {/* Header - Show max 4 columns */}
                {Object.keys(dataset.preview[0]).slice(0, 4).map((col, idx) => (
                  <div key={idx} className="dataset-preview-cell" style={{ 
                    fontWeight: 'bold', 
                    color: '#CED9E0',
                    padding: '3px 4px',
                    backgroundColor: '#30404D',
                    borderRadius: '2px',
                    textAlign: 'center',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    minWidth: '60px',
                    maxWidth: '80px'
                  }}>
                    {col.length > 8 ? col.substring(0, 6) + '..' : col}
                  </div>
                ))}
                
                {/* Show "..." if more columns */}
                {Object.keys(dataset.preview[0]).length > 4 && (
                  <div style={{ 
                    fontWeight: 'bold', 
                    color: '#8A9BA8',
                    padding: '3px 4px',
                    backgroundColor: '#30404D',
                    borderRadius: '2px',
                    textAlign: 'center',
                    minWidth: '30px'
                  }}>
                    +{Object.keys(dataset.preview[0]).length - 4}
                  </div>
                )}
                
                {/* Data Rows - Show max 2 rows and 4 columns */}
                {dataset.preview.slice(0, 2).map((row, rowIdx) => 
                  Object.values(row).slice(0, 4).map((value, colIdx) => (
                    <div key={`${rowIdx}-${colIdx}`} className="dataset-preview-cell" style={{ 
                      color: '#A7B6C2',
                      padding: '3px 4px',
                      backgroundColor: '#2F3F4C',
                      borderRadius: '2px',
                      textAlign: 'center',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      minWidth: '60px',
                      maxWidth: '80px'
                    }}>
                      {String(value).length > 8 ? String(value).substring(0, 6) + '..' : String(value)}
                    </div>
                  ))
                )}
                
                {/* Show "..." for rows if more than 4 columns */}
                {Object.keys(dataset.preview[0]).length > 4 && dataset.preview.slice(0, 2).map((row, rowIdx) => (
                  <div key={`${rowIdx}-more`} style={{ 
                    color: '#8A9BA8',
                    padding: '3px 4px',
                    backgroundColor: '#2F3F4C',
                    borderRadius: '2px',
                    textAlign: 'center',
                    minWidth: '30px'
                  }}>
                    ...
                  </div>
                ))}
              </div>
            </div>
            
            <div style={{ 
              marginTop: '8px', 
              textAlign: 'center', 
              color: '#8A9BA8',
              fontSize: '9px',
              fontStyle: 'italic'
            }}>
              {dataset.preview.length > 2 && `+${dataset.preview.length - 2} more rows`}
              {dataset.preview.length > 2 && Object.keys(dataset.preview[0]).length > 4 && ' • '}
              {Object.keys(dataset.preview[0]).length > 4 && `${Object.keys(dataset.preview[0]).length} total columns`}
            </div>
          </Card>
        </div>
      )}

      <Button
        onClick={() => loadDataset(dataset.filename, isUploaded)}
        disabled={loading}
        intent={Intent.PRIMARY}
        fill={true}
        loading={loading}
        text={loading ? 'Loading...' : 'Select Dataset'}
        style={{ 
          marginTop: '4px',
          height: '36px',
          fontSize: '14px',
          fontWeight: '500'
        }}
      />
    </Card>
  );

  return (
    <Card elevation={Elevation.TWO} style={{ 
      backgroundColor: '#394B59',
      border: '1px solid #5C7080',
      borderRadius: '8px'
    }}>
      <div className="card-container" style={{ padding: '24px' }}>
        <H3 style={{ 
          display: 'flex', 
          alignItems: 'center', 
          marginBottom: '24px',
          color: '#F5F8FA',
          fontSize: '18px',
          fontWeight: '600'
        }}>
          <span className={`bp5-icon bp5-icon-database`} style={{ marginRight: '12px', fontSize: '18px' }} />
          Dataset Selection
        </H3>

        {/* Tabs */}
        <Tabs
          id="dataset-tabs"
          selectedTabId={activeTab}
          onChange={(tabId) => {
            logger.info('Tab switched', {
              operation: 'tab_switch',
              component: 'DatasetSelector',
              new_tab: tabId,
              previous_tab: activeTab,
              user_action: 'tab_navigation'
            });
            setActiveTab(tabId as 'sample' | 'upload' | 's3');
          }}
          style={{ marginBottom: '24px' }}
        >
          <Tab
            id="sample"
            title={
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <span className={`bp5-icon bp5-icon-document`} style={{ marginRight: '5px' }} />
                Sample Datasets ({sampleDatasets.length})
              </div>
            }
          />
          <Tab
            id="upload"
            title={
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <span className={`bp5-icon bp5-icon-upload`} style={{ marginRight: '5px' }} />
                Uploaded Files ({uploadedFiles.length})
              </div>
            }
          />
          <Tab
            id="s3"
            title={
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <span className={`bp5-icon bp5-icon-cloud`} style={{ marginRight: '5px' }} />
                AWS S3 Datasets
              </div>
            }
          />
        </Tabs>

        {/* Content */}
        {activeTab === 'sample' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            {sampleDatasets.length === 0 ? (
              <Text className={Classes.TEXT_MUTED} style={{ 
                textAlign: 'center', 
                padding: '48px 24px',
                fontSize: '14px',
                color: '#8A9BA8'
              }}>
                No sample datasets available
              </Text>
            ) : (
              sampleDatasets.map((dataset) => (
                <DatasetCard key={dataset.filename} dataset={dataset} />
              ))
            )}
          </div>
        )}

        {activeTab === 'upload' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '18px' }}>
            {/* Error Message */}
            {uploadError && (
              <Callout intent={Intent.DANGER} title="Upload Error" icon={IconNames.ERROR}>
                {uploadError}
              </Callout>
            )}
            
            {/* Upload Area */}
            <Card
              elevation={dragOver ? Elevation.THREE : Elevation.ONE}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              style={{
                padding: '36px',
                textAlign: 'center',
                border: dragOver ? '2px dashed #48AFF0' : '2px dashed #5C7080',
                backgroundColor: dragOver ? '#2F3F4C' : '#30404D',
                borderRadius: '8px',
                transition: 'all 0.2s ease'
              }}
            >
              <span className={`bp5-icon bp5-icon-upload`} style={{ 
                fontSize: '48px', 
                marginBottom: '16px', 
                color: dragOver ? '#48AFF0' : '#8A9BA8',
                display: 'block'
              }} />
              <H5 style={{ 
                marginBottom: '8px', 
                color: '#F5F8FA',
                fontSize: '16px',
                fontWeight: '600'
              }}>
                Drop CSV files here
              </H5>
              <Text className={Classes.TEXT_MUTED} style={{ 
                marginBottom: '20px',
                fontSize: '13px'
              }}>
                or click to browse files
              </Text>
              <FileInput
                text={uploadLoading ? (uploadProgress || 'Uploading...') : 'Choose File'}
                onInputChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) handleFileUpload(file);
                }}
                inputProps={{ accept: '.csv' }}
                disabled={uploadLoading}
                style={{ maxWidth: '200px', margin: '0 auto' }}
              />
              {uploadLoading && uploadProgress && (
                <div style={{ marginTop: '12px' }}>
                  <Text className={Classes.TEXT_SMALL} style={{ color: '#A7B6C2' }}>
                    {uploadProgress}
                  </Text>
                </div>
              )}
            </Card>

            {/* Uploaded Files List */}
            {uploadedFiles.length === 0 ? (
              <Text className={Classes.TEXT_MUTED} style={{ 
                textAlign: 'center', 
                padding: '48px 24px',
                fontSize: '14px',
                color: '#8A9BA8'
              }}>
                No uploaded files yet
              </Text>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                {uploadedFiles.map((dataset) => (
                  <DatasetCard key={dataset.filename} dataset={dataset} isUploaded={true} />
                ))}
              </div>
            )}
          </div>
        )}

        {activeTab === 's3' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            {s3Checking ? (
              <div style={{ textAlign: 'center', padding: '48px 24px' }}>
                <Spinner size={40} />
                <Text style={{ marginTop: '16px', color: '#A7B6C2' }}>
                  Checking AWS S3 connection...
                </Text>
              </div>
            ) : !s3Connected ? (
              <NonIdealState
                icon={IconNames.CLOUD_OFFLINE}
                title="AWS S3 Not Connected"
                description="To access S3 datasets, you need to configure AWS credentials and connection settings."
                action={
                  <div style={{ marginTop: '24px' }}>
                    <Callout intent={Intent.WARNING} icon={IconNames.WARNING_SIGN}>
                      <H5>Connection Required</H5>
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
            ) : (
              <Text className={Classes.TEXT_MUTED} style={{ 
                textAlign: 'center', 
                padding: '48px 24px',
                fontSize: '14px',
                color: '#8A9BA8'
              }}>
                S3 datasets feature coming soon...
              </Text>
            )}
          </div>
        )}
      </div>
    </Card>
  );
}