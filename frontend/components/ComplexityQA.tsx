'use client';

import { useState } from 'react';
import { 
  Button, 
  Card, 
  Elevation, 
  H5, 
  Text, 
  Intent, 
  Callout,
  TextArea,
  Spinner,
  Tag,
  Divider,
  Icon
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';

interface ComplexityQAProps {
  analysis: any;
}

interface QAHistory {
  id: string;
  question: string;
  answer: string;
  timestamp: Date;
  response_time?: number;
}

export default function ComplexityQA({ analysis }: ComplexityQAProps) {
  const [question, setQuestion] = useState('');
  const [loading, setLoading] = useState(false);
  const [qaHistory, setQaHistory] = useState<QAHistory[]>([]);
  const [error, setError] = useState('');

  const suggestedQuestions = [
    "Why does my query have a high complexity score?",
    "How can I optimize this query?",
    "What makes JOINs expensive?",
    "Explain my memory usage",
    "What are window functions?",
    "How accurate are the time estimates?"
  ];

  const askQuestion = async (questionText: string) => {
    if (!questionText.trim()) return;

    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/ask-complexity-question', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          question: questionText,
          analysis_context: analysis
        })
      });

      const data = await response.json();

      if (data.success) {
        const newQA: QAHistory = {
          id: Date.now().toString(),
          question: questionText,
          answer: data.answer,
          timestamp: new Date(),
          response_time: data.response_time
        };

        setQaHistory(prev => [newQA, ...prev]);
        setQuestion('');
      } else {
        setError(data.error || 'Failed to get answer');
      }
    } catch (err) {
      setError('Network error - please try again');
      console.error('QA Error:', err);
    } finally {
      setLoading(false);
    }
  };

  const getOptimizationSuggestions = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/optimize-query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: analysis.query || '',
          analysis: analysis
        })
      });

      const data = await response.json();

      if (data.success) {
        const newQA: QAHistory = {
          id: Date.now().toString(),
          question: "How can I optimize this query?",
          answer: data.optimization_suggestions,
          timestamp: new Date(),
          response_time: data.response_time
        };

        setQaHistory(prev => [newQA, ...prev]);
      } else {
        setError(data.error || 'Failed to get optimization suggestions');
      }
    } catch (err) {
      setError('Network error - please try again');
      console.error('Optimization Error:', err);
    } finally {
      setLoading(false);
    }
  };

  const explainScore = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/explain-score', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          analysis: analysis
        })
      });

      const data = await response.json();

      if (data.success) {
        const newQA: QAHistory = {
          id: Date.now().toString(),
          question: "Why did my query get this complexity score?",
          answer: data.explanation,
          timestamp: new Date(),
          response_time: data.response_time
        };

        setQaHistory(prev => [newQA, ...prev]);
      } else {
        setError(data.error || 'Failed to get score explanation');
      }
    } catch (err) {
      setError('Network error - please try again');
      console.error('Score explanation Error:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      {/* Quick Actions */}
      <div style={{ marginBottom: '20px' }}>
        <div style={{ display: 'flex', gap: '10px', marginBottom: '15px', flexWrap: 'wrap' }}>
          <Button
            intent={Intent.PRIMARY}
            icon={IconNames.LIGHTBULB}
            onClick={getOptimizationSuggestions}
            loading={loading}
            small
          >
            Get Optimization Tips
          </Button>
          <Button
            intent={Intent.SUCCESS}
            icon={IconNames.INFO_SIGN}
            onClick={explainScore}
            loading={loading}
            small
          >
            Explain My Score
          </Button>
        </div>
      </div>

      {/* Custom Question Input */}
      <Card elevation={Elevation.ONE} style={{ 
        padding: '15px',
        backgroundColor: '#30404D',
        border: '1px solid #5C7080',
        marginBottom: '20px'
      }}>
        <div style={{ marginBottom: '10px' }}>
          <Text style={{ fontWeight: 'bold', color: '#F5F8FA' }}>Ask a Custom Question:</Text>
        </div>
        <TextArea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Type your question about the complexity analysis..."
          fill
          rows={2}
          style={{ 
            marginBottom: '10px',
            backgroundColor: '#394B59',
            border: '1px solid #5C7080',
            color: '#F5F8FA'
          }}
          disabled={loading}
        />
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Button
            intent={Intent.PRIMARY}
            onClick={() => askQuestion(question)}
            disabled={!question.trim() || loading}
            loading={loading}
            small
          >
            {loading ? 'Asking AI...' : 'Ask Question'}
          </Button>
          {loading && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
              <Spinner size={16} />
              <Text style={{ fontSize: '12px', color: '#A7B6C2' }}>
                AI is thinking...
              </Text>
            </div>
          )}
        </div>
      </Card>

      {/* Suggested Questions */}
      <div style={{ marginBottom: '20px' }}>
        <Text style={{ fontWeight: 'bold', marginBottom: '10px', color: '#F5F8FA' }}>
          Suggested Questions:
        </Text>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
          {suggestedQuestions.map((suggestedQ, index) => (
            <Tag
              key={index}
              interactive
              onClick={() => askQuestion(suggestedQ)}
              style={{ cursor: 'pointer', fontSize: '11px' }}
              intent={Intent.NONE}
            >
              {suggestedQ}
            </Tag>
          ))}
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <Callout intent={Intent.DANGER} style={{ marginBottom: '20px' }}>
          <Text>{error}</Text>
        </Callout>
      )}

      {/* Q&A History */}
      {qaHistory.length > 0 && (
        <div>
          <H5 style={{ 
            marginBottom: '15px',
            color: '#F5F8FA',
            display: 'flex',
            alignItems: 'center'
          }}>
            <Icon icon={IconNames.HISTORY} size={16} style={{ marginRight: '8px' }} />
            Q&A History
          </H5>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
            {qaHistory.map((qa) => (
              <Card 
                key={qa.id} 
                elevation={Elevation.ONE}
                style={{ 
                  padding: '16px',
                  backgroundColor: '#30404D',
                  border: '1px solid #5C7080'
                }}
              >
                <div style={{ marginBottom: '12px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '8px' }}>
                    <Text style={{ 
                      fontWeight: 'bold', 
                      color: '#48AFF0',
                      fontSize: '14px'
                    }}>
                      Q: {qa.question}
                    </Text>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                      {qa.response_time && (
                        <Tag minimal style={{ fontSize: '10px' }}>
                          {qa.response_time.toFixed(1)}s
                        </Tag>
                      )}
                      <Text style={{ 
                        fontSize: '11px', 
                        color: '#A7B6C2'
                      }}>
                        {qa.timestamp.toLocaleTimeString()}
                      </Text>
                    </div>
                  </div>
                  <Divider style={{ margin: '8px 0' }} />
                  <Text style={{ 
                    color: '#F5F8FA',
                    lineHeight: '1.5',
                    fontSize: '13px',
                    whiteSpace: 'pre-wrap'
                  }}>
                    {qa.answer}
                  </Text>
                </div>
              </Card>
            ))}
          </div>
        </div>
      )}

      {/* Empty State */}
      {qaHistory.length === 0 && !loading && (
        <Callout intent={Intent.NONE} style={{ textAlign: 'center' }}>
          <Icon icon={IconNames.CHAT} size={24} style={{ marginBottom: '10px' }} />
          <Text>
            Ask questions about your query complexity analysis to get personalized AI explanations!
          </Text>
        </Callout>
      )}
    </div>
  );
}