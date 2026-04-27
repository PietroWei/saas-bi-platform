backend: cd backend && uvicorn main:app --host 0.0.0.0 --port $PORT
frontend: cd frontend && streamlit run app/main.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false
