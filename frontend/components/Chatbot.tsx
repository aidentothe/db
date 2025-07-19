'use client';

import { useState, useRef, useEffect } from 'react';
import {
  Card,
  Elevation,
  Button,
  Intent,
  InputGroup,
  Text,
  Classes,
  Icon,
  Spinner
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';

interface Message {
  id: string;
  text: string;
  isUser: boolean;
  timestamp: Date;
}

interface ChatbotProps {
  position?: 'bottom-right' | 'bottom-left';
}

export default function Chatbot({ position = 'bottom-right' }: ChatbotProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      text: 'Hello! I\'m here to help you with Spark SQL queries and Catalyst optimizer questions. What would you like to know?',
      isUser: false,
      timestamp: new Date()
    }
  ]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const generateResponse = async (userMessage: string): Promise<string> => {
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 2000));
    
    const lowerMessage = userMessage.toLowerCase();
    
    if (lowerMessage.includes('catalyst') || lowerMessage.includes('optimizer')) {
      return 'The Catalyst optimizer is Spark\'s rule-based query optimization framework. It applies various optimization rules like predicate pushdown, column pruning, and constant folding to improve query performance. Would you like me to explain any specific optimization rule?';
    }
    
    if (lowerMessage.includes('performance') || lowerMessage.includes('slow')) {
      return 'For query performance issues, I recommend: 1) Check your execution plans for expensive operations, 2) Ensure proper partitioning strategy, 3) Consider data skew issues, 4) Use broadcast joins for small tables, 5) Enable adaptive query execution (AQE). What specific performance issue are you facing?';
    }
    
    if (lowerMessage.includes('join') || lowerMessage.includes('broadcast')) {
      return 'Spark supports several join strategies: 1) Broadcast Hash Join (best for small tables), 2) Sort Merge Join (for large tables), 3) Shuffle Hash Join. The optimizer automatically chooses based on table sizes and statistics. You can force broadcast with broadcast() function.';
    }
    
    if (lowerMessage.includes('partition') || lowerMessage.includes('bucketing')) {
      return 'Proper partitioning is crucial for performance. Consider: 1) Partition by columns frequently used in WHERE clauses, 2) Avoid too many small partitions, 3) Use bucketing for join optimization, 4) Consider partition pruning benefits. What\'s your data layout?';
    }
    
    if (lowerMessage.includes('sql') || lowerMessage.includes('query')) {
      return 'I can help optimize your SQL queries! Common optimizations include: using appropriate WHERE clauses, leveraging window functions efficiently, proper JOIN order, and using EXPLAIN to understand execution plans. Share your query and I\'ll provide specific suggestions.';
    }
    
    if (lowerMessage.includes('error') || lowerMessage.includes('fail')) {
      return 'Common Spark SQL errors include: 1) Out of memory issues (try increasing driver/executor memory), 2) Data skew (repartition or use salting), 3) Schema mismatches (check data types), 4) Timeout issues (adjust spark.sql.adaptive settings). What error are you encountering?';
    }
    
    if (lowerMessage.includes('hello') || lowerMessage.includes('help')) {
      return 'I\'m your Spark SQL assistant! I can help with query optimization, Catalyst optimizer rules, performance tuning, join strategies, partitioning advice, and troubleshooting. What would you like to explore?';
    }
    
    // Generic responses for unmatched queries
    const genericResponses = [
      'That\'s an interesting question about Spark SQL! Could you provide more specific details so I can give you a more targeted answer?',
      'I\'d be happy to help with that. Could you share more context about what you\'re trying to achieve with your Spark SQL query?',
      'Let me help you with that Spark-related question. Could you provide more details about your specific use case?',
      'Great question! For the best advice, could you share more about your data structure and what you\'re trying to accomplish?'
    ];
    
    return genericResponses[Math.floor(Math.random() * genericResponses.length)];
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      text: inputValue,
      isUser: true,
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const response = await generateResponse(inputValue);
      const botMessage: Message = {
        id: (Date.now() + 1).toString(),
        text: response,
        isUser: false,
        timestamp: new Date()
      };
      setMessages(prev => [...prev, botMessage]);
    } catch (error) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        text: 'Sorry, I encountered an error. Please try again.',
        isUser: false,
        timestamp: new Date()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  const positionStyles = position === 'bottom-right' 
    ? { bottom: '24px', right: '24px' }
    : { bottom: '24px', left: '24px' };

  return (
    <div style={{
      position: 'fixed',
      ...positionStyles,
      zIndex: 1000,
      display: 'flex',
      flexDirection: 'column',
      alignItems: position === 'bottom-right' ? 'flex-end' : 'flex-start'
    }}>
      {/* Chat Window */}
      {isExpanded && (
        <Card
          elevation={Elevation.FOUR}
          style={{
            width: '380px',
            height: '500px',
            marginBottom: '16px',
            backgroundColor: '#30404D',
            border: '1px solid #394B59',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden'
          }}
        >
          {/* Header */}
          <div style={{
            padding: '16px',
            borderBottom: '1px solid #394B59',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            backgroundColor: '#394B59'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Icon icon={IconNames.CHAT} size={16} style={{ color: '#48AFF0' }} />
              <Text style={{ fontWeight: 'bold', color: '#F5F8FA' }}>
                Spark SQL Assistant
              </Text>
            </div>
            <Button
              minimal
              small
              icon={IconNames.CROSS}
              onClick={() => setIsExpanded(false)}
              style={{ color: '#A7B6C2' }}
            />
          </div>

          {/* Messages */}
          <div style={{
            flex: 1,
            padding: '16px',
            overflowY: 'auto',
            display: 'flex',
            flexDirection: 'column',
            gap: '12px'
          }}>
            {messages.map((message) => (
              <div
                key={message.id}
                style={{
                  display: 'flex',
                  justifyContent: message.isUser ? 'flex-end' : 'flex-start'
                }}
              >
                <div
                  style={{
                    maxWidth: '80%',
                    padding: '8px 12px',
                    borderRadius: '12px',
                    backgroundColor: message.isUser ? '#48AFF0' : '#394B59',
                    color: message.isUser ? '#FFFFFF' : '#F5F8FA',
                    fontSize: '14px',
                    lineHeight: '1.4'
                  }}
                >
                  {message.text}
                </div>
              </div>
            ))}
            {isLoading && (
              <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
                <div style={{
                  padding: '8px 12px',
                  borderRadius: '12px',
                  backgroundColor: '#394B59',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px'
                }}>
                  <Spinner size={16} />
                  <Text style={{ color: '#A7B6C2', fontSize: '14px' }}>
                    Thinking...
                  </Text>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div style={{
            padding: '16px',
            borderTop: '1px solid #394B59',
            backgroundColor: '#30404D'
          }}>
            <div style={{ display: 'flex', gap: '8px' }}>
              <InputGroup
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="Ask about Spark SQL, Catalyst, performance..."
                disabled={isLoading}
                style={{ flex: 1 }}
                className={Classes.DARK}
              />
              <Button
                intent={Intent.PRIMARY}
                icon={IconNames.SEND_MESSAGE}
                onClick={handleSendMessage}
                disabled={!inputValue.trim() || isLoading}
                minimal
              />
            </div>
          </div>
        </Card>
      )}

      {/* Toggle Button */}
      <Button
        intent={Intent.PRIMARY}
        icon={isExpanded ? IconNames.CHEVRON_DOWN : IconNames.CHAT}
        onClick={() => setIsExpanded(!isExpanded)}
        large
        style={{
          borderRadius: '50%',
          width: '56px',
          height: '56px',
          boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
          backgroundColor: '#48AFF0',
          border: 'none'
        }}
        title={isExpanded ? 'Close chat' : 'Open Spark SQL Assistant'}
      />
    </div>
  );
}