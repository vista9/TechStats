from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
import requests
import time
import re
from threading import Lock
from datetime import datetime
import concurrent.futures
import uvicorn

app = FastAPI(
    title="TechStats API",
    description="API для анализа технологий в вакансиях с HH.ru",
    version="1.0.0"
)

# CORS настройки
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальные настройки
MAX_WORKERS = 5
REQUEST_DELAY = 0.1
MAX_REQUESTS_PER_SECOND = 7
HH_API_BASE_URL = "https://api.hh.ru"

# Глобальные счетчики и кэш
request_counter = 0
cached_requests_counter = 0
counter_lock = Lock()
last_request_time = time.time()
rate_limit_lock = Lock()
description_cache = {}
cache_lock = Lock()

# Модели данных
class VacancySearchRequest(BaseModel):
    vacancy_title: str = Field(..., description="Название вакансии", example="Data Engineer")
    technology: str = Field(..., description="Технология для поиска", example="Python")
    exact_search: bool = Field(default=True, description="Точный поиск по названию вакансии")
    area: int = Field(default=113, description="ID региона (113 - Россия)")
    max_pages: int = Field(default=10, description="Максимальное количество страниц для загрузки")

class VacancyInfo(BaseModel):
    id: str
    name: str
    url: str

class AnalysisResult(BaseModel):
    vacancy_title: str
    technology: str
    total_vacancies: int
    tech_vacancies: int
    tech_percentage: float
    vacancies_with_tech: List[VacancyInfo]
    analysis_timestamp: str
    request_stats: Dict

# Утилиты для работы с API
def increment_request_counter(use_cache=False):
    global request_counter, cached_requests_counter, last_request_time
    
    with counter_lock:
        if use_cache:
            cached_requests_counter += 1
        else:
            request_counter += 1
            current_time = time.time()
            
            with rate_limit_lock:
                time_since_last_request = current_time - last_request_time
                if time_since_last_request < (1.0 / MAX_REQUESTS_PER_SECOND):
                    sleep_time = (1.0 / MAX_REQUESTS_PER_SECOND) - time_since_last_request
                    time.sleep(sleep_time)
                
                last_request_time = time.time()

def get_request_count():
    with counter_lock:
        return request_counter, cached_requests_counter

def reset_request_counters():
    global request_counter, cached_requests_counter
    with counter_lock:
        request_counter = 0
        cached_requests_counter = 0

def get_vacancy_description_cached(vacancy_id: str) -> str:
    with cache_lock:
        if vacancy_id in description_cache:
            increment_request_counter(use_cache=True)
            return description_cache[vacancy_id]
    
    try:
        url = f"{HH_API_BASE_URL}/vacancies/{vacancy_id}"
        response = requests.get(url, timeout=10)
        increment_request_counter(use_cache=False)
        response.raise_for_status()
        data = response.json()
        description = data.get('description', '')
        
        with cache_lock:
            description_cache[vacancy_id] = description
        
        return description
    except:
        with cache_lock:
            description_cache[vacancy_id] = ""
        return ""

def fetch_single_page(search_text: str, area: int, per_page: int, page: int) -> List[Dict]:
    params = {
        'text': search_text,
        'search_field': 'name',
        'area': area,
        'per_page': per_page,
        'page': page,
        'only_with_salary': False
    }
    
    response = requests.get(f"{HH_API_BASE_URL}/vacancies", params=params, timeout=10)
    increment_request_counter(use_cache=False)
    response.raise_for_status()
    data = response.json()
    time.sleep(REQUEST_DELAY)
    
    return data.get('items', [])

def get_vacancies(search_text: str, area: int, max_pages: int) -> List[Dict]:
    reset_request_counters()
    
    params = {
        'text': search_text,
        'search_field': 'name',
        'area': area,
        'per_page': 100,
        'page': 0,
        'only_with_salary': False
    }
    
    try:
        response = requests.get(f"{HH_API_BASE_URL}/vacancies", params=params, timeout=10)
        increment_request_counter(use_cache=False)
        response.raise_for_status()
        data = response.json()
        
        total_pages = min(data.get('pages', 1), max_pages)
        vacancies = data.get('items', [])
        
        if total_pages > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_page = {
                    executor.submit(fetch_single_page, search_text, area, 100, page): page 
                    for page in range(1, total_pages)
                }
                
                for future in concurrent.futures.as_completed(future_to_page):
                    try:
                        page_vacancies = future.result()
                        vacancies.extend(page_vacancies)
                    except Exception as e:
                        print(f"Ошибка при загрузке страницы: {e}")
        
        return vacancies
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении вакансий: {str(e)}")

def check_vacancy_for_tech(vacancy: Dict, tech_pattern) -> Dict:
    vacancy_id = vacancy.get('id')
    vacancy_name = vacancy.get('name', '')
    vacancy_url = vacancy.get('alternate_url', '')
    
    # Проверяем название
    if tech_pattern.search(vacancy_name.lower() if vacancy_name else ""):
        return {
            'has_tech': True,
            'vacancy_info': {
                'id': vacancy_id,
                'name': vacancy_name,
                'url': vacancy_url
            }
        }
    
    # Проверяем сниппет
    snippet = vacancy.get('snippet', {}) or {}
    requirement = snippet.get('requirement', '') or ''
    responsibility = snippet.get('responsibility', '') or ''
    snippet_text = (requirement + " " + responsibility).lower()
    
    if tech_pattern.search(snippet_text):
        return {
            'has_tech': True,
            'vacancy_info': {
                'id': vacancy_id,
                'name': vacancy_name,
                'url': vacancy_url
            }
        }
    
    # Проверяем полное описание
    description = get_vacancy_description_cached(vacancy_id).lower()
    if tech_pattern.search(description):
        return {
            'has_tech': True,
            'vacancy_info': {
                'id': vacancy_id,
                'name': vacancy_name,
                'url': vacancy_url
            }
        }
    
    return {'has_tech': False, 'vacancy_info': None}

def analyze_vacancies(vacancies: List[Dict], technology: str) -> Dict:
    tech_pattern = re.compile(r'\b' + re.escape(technology.lower()) + r'\b', re.IGNORECASE)
    tech_vacancies_details = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_vacancy = {
            executor.submit(check_vacancy_for_tech, vacancy, tech_pattern): vacancy 
            for vacancy in vacancies
        }
        
        for future in concurrent.futures.as_completed(future_to_vacancy):
            try:
                result = future.result()
                if result['has_tech']:
                    tech_vacancies_details.append(result['vacancy_info'])
            except Exception as e:
                print(f"Ошибка при анализе вакансии: {e}")
    
    total_vacancies = len(vacancies)
    tech_vacancies = len(tech_vacancies_details)
    
    return {
        'total_vacancies': total_vacancies,
        'tech_vacancies': tech_vacancies,
        'tech_percentage': (tech_vacancies / total_vacancies * 100) if total_vacancies > 0 else 0,
        'tech_vacancies_details': tech_vacancies_details
    }

# Endpoints
@app.get("/")
async def root():
    return {
        "service": "TechStats API",
        "version": "1.0.0",
        "description": "API для анализа технологий в вакансиях с HH.ru"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(description_cache)
    }

@app.post("/analyze", response_model=AnalysisResult)
async def analyze_technology(request: VacancySearchRequest):
    """
    Основной endpoint для анализа вакансий по технологии
    """
    # Формируем поисковый запрос
    if request.exact_search:
        search_query = f'"{request.vacancy_title}"'
    else:
        search_query = request.vacancy_title
    
    # Получаем вакансии
    vacancies = get_vacancies(
        search_text=search_query,
        area=request.area,
        max_pages=request.max_pages
    )
    
    if not vacancies:
        raise HTTPException(status_code=404, detail="Вакансии не найдены")
    
    # Анализируем вакансии
    stats = analyze_vacancies(vacancies, request.technology)
    
    # Получаем статистику запросов
    real_requests, cached_requests = get_request_count()
    
    return AnalysisResult(
        vacancy_title=request.vacancy_title,
        technology=request.technology,
        total_vacancies=stats['total_vacancies'],
        tech_vacancies=stats['tech_vacancies'],
        tech_percentage=round(stats['tech_percentage'], 2),
        vacancies_with_tech=[
            VacancyInfo(**v) for v in stats['tech_vacancies_details']
        ],
        analysis_timestamp=datetime.now().isoformat(),
        request_stats={
            'real_requests': real_requests,
            'cached_requests': cached_requests,
            'total_requests': real_requests + cached_requests,
            'cache_size': len(description_cache)
        }
    )

@app.get("/cache/stats")
async def get_cache_statistics():
    """
    Получить статистику кэша
    """
    real_requests, cached_requests = get_request_count()
    total = real_requests + cached_requests
    
    return {
        "cache_size": len(description_cache),
        "real_requests": real_requests,
        "cached_requests": cached_requests,
        "total_requests": total,
        "cache_hit_rate": (cached_requests / total * 100) if total > 0 else 0
    }

@app.delete("/cache/clear")
async def clear_cache():
    """
    Очистить кэш
    """
    with cache_lock:
        cache_size = len(description_cache)
        description_cache.clear()
    
    reset_request_counters()
    
    return {
        "message": "Кэш успешно очищен",
        "cleared_items": cache_size
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)