import os
import logging
import json
from typing import Dict, Any, Optional
import requests

class GroqClient:
    """Client for interacting with Groq API for complexity analysis Q&A"""
    
    def __init__(self, api_key: str = None):
        """
        Initialize Groq client
        
        Args:
            api_key: Groq API key (defaults to environment variable)
        """
        self.api_key = api_key or os.getenv('GROQ_API_KEY', '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')
        self.base_url = "https://api.groq.com/openai/v1"
        self.model = "llama-3.3-70b-versatile"  # Best model for our technical Q&A needs
        
        self.logger = logging.getLogger(__name__)
        
    def _make_request(self, messages, temperature=0.7, max_tokens=2048):
        """Make a request to Groq API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "top_p": 0.9
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Groq API request failed: {e}")
            raise
        
    def ask_complexity_question(self, question: str, analysis_context: Dict[str, Any] = None) -> str:
        """
        Ask a question about SQL query complexity analysis
        
        Args:
            question: User's question about complexity analysis
            analysis_context: Current complexity analysis data for context
            
        Returns:
            AI-generated answer explaining the complexity concept
        """
        try:
            # Build context-aware system prompt
            system_prompt = self._build_system_prompt(analysis_context)
            
            # Create the conversation
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ]
            
            # Get response from Groq
            response_data = self._make_request(messages)
            answer = response_data['choices'][0]['message']['content'].strip()
            
            self.logger.info(f"Groq complexity question answered", extra={
                'operation': 'groq_complexity_qa',
                'question_length': len(question),
                'answer_length': len(answer),
                'model': self.model,
                'has_context': analysis_context is not None
            })
            
            return answer
            
        except Exception as e:
            self.logger.error(f"Error with Groq complexity Q&A: {str(e)}", extra={
                'operation': 'groq_complexity_qa_error',
                'error_type': type(e).__name__,
                'question': question[:100]  # First 100 chars for debugging
            })
            return f"I apologize, but I encountered an error while processing your question: {str(e)}"
    
    def _build_system_prompt(self, analysis_context: Dict[str, Any] = None) -> str:
        """Build a context-aware system prompt for complexity Q&A"""
        
        base_prompt = """You are an expert SQL performance analyst and database optimization specialist. Your role is to help users understand SQL query complexity analysis in clear, practical terms.

Key areas of expertise:
- SQL query complexity scoring (compute and memory)
- JOIN operations and their performance implications
- Subquery vs JOIN trade-offs
- Window functions and their use cases
- Query optimization strategies
- Database indexing recommendations
- Performance estimation accuracy
- Memory usage patterns in SQL operations

Guidelines for responses:
1. Use clear, non-technical language when possible
2. Provide practical examples and scenarios
3. Explain the "why" behind performance impacts
4. Offer actionable optimization suggestions
5. Be concise but thorough
6. Include relevant SQL examples when helpful
7. Address both performance and readability concerns

When users ask about their specific query analysis, reference the provided context to give personalized explanations."""

        if analysis_context:
            context_info = f"""

CURRENT QUERY ANALYSIS CONTEXT:
- Overall Complexity Rating: {analysis_context.get('complexity_rating', 'N/A')}/10
- Compute Score: {analysis_context.get('compute_score', 'N/A')}/10
- Memory Score: {analysis_context.get('memory_score', 'N/A')}/10
- Query Components: {analysis_context.get('components', {})}
- Performance Estimate: {analysis_context.get('performance_estimate', {})}
- Optimization Suggestions: {analysis_context.get('optimization_suggestions', [])}

Use this context to provide personalized explanations about the user's specific query."""

            return base_prompt + context_info
        
        return base_prompt
    
    def generate_optimization_suggestions(self, query: str, analysis: Dict[str, Any]) -> str:
        """
        Generate detailed optimization suggestions for a specific query
        
        Args:
            query: The SQL query to optimize
            analysis: Complexity analysis results
            
        Returns:
            Detailed optimization recommendations
        """
        try:
            prompt = f"""Given this SQL query and its complexity analysis, provide specific optimization recommendations:

QUERY:
{query}

ANALYSIS:
- Complexity Rating: {analysis.get('complexity_rating', 'N/A')}/10
- Compute Score: {analysis.get('compute_score', 'N/A')}/10  
- Memory Score: {analysis.get('memory_score', 'N/A')}/10
- Components: {analysis.get('components', {})}
- Current Suggestions: {analysis.get('optimization_suggestions', [])}

Please provide:
1. Specific optimization techniques for this query
2. Alternative query structures to consider
3. Indexing recommendations
4. Potential trade-offs to be aware of
5. Expected performance improvements

Focus on actionable, practical advice."""

            messages = [
                {"role": "system", "content": "You are a SQL optimization expert. Provide specific, actionable optimization advice."},
                {"role": "user", "content": prompt}
            ]
            
            response_data = self._make_request(messages, temperature=0.5)
            return response_data['choices'][0]['message']['content'].strip()
            
        except Exception as e:
            self.logger.error(f"Error generating optimization suggestions: {str(e)}")
            return f"Unable to generate optimization suggestions: {str(e)}"
    
    def explain_complexity_score(self, analysis: Dict[str, Any]) -> str:
        """
        Provide a detailed explanation of why a query received its complexity score
        
        Args:
            analysis: Complexity analysis results
            
        Returns:
            Detailed explanation of the complexity scoring
        """
        try:
            prompt = f"""Explain in detail why this SQL query received its complexity scores:

COMPLEXITY ANALYSIS:
- Overall Rating: {analysis.get('complexity_rating', 'N/A')}/10
- Compute Score: {analysis.get('compute_score', 'N/A')}/10
- Memory Score: {analysis.get('memory_score', 'N/A')}/10
- Query Components: {analysis.get('components', {})}
- Performance Estimate: {analysis.get('performance_estimate', {})}

Please explain:
1. What factors contributed to the compute score
2. What factors contributed to the memory score  
3. Which specific query components are most impactful
4. How these scores translate to real-world performance
5. What makes this query complex or simple

Make it educational and help the user understand the scoring methodology."""

            messages = [
                {"role": "system", "content": "You are a database performance educator. Explain complexity scoring in clear, educational terms."},
                {"role": "user", "content": prompt}
            ]
            
            response_data = self._make_request(messages, temperature=0.6)
            return response_data['choices'][0]['message']['content'].strip()
            
        except Exception as e:
            self.logger.error(f"Error explaining complexity score: {str(e)}")
            return f"Unable to explain complexity score: {str(e)}"