from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict
import requests
import time
import re
from threading import Lock
from datetime import datetime, timedelta
import uvicorn
import asyncio
import functools

app = FastAPI(
    title="TechStats API",
    description="API для анализа технологий в вакансиях с HH.ru",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None  # Отключаем ReDoc
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
CACHE_TTL_HOURS = 24  # Время жизни кэша в часах

# Глобальные счетчики и кэш
request_counter = 0
cached_requests_counter = 0
counter_lock = Lock()
last_request_time = time.time()
rate_limit_lock = Lock()

# Улучшенный кэш с временными метками
class CacheEntry:
    def __init__(self, data: str, timestamp: datetime):
        self.data = data
        self.timestamp = timestamp
    
    def is_expired(self, ttl_hours: int = CACHE_TTL_HOURS) -> bool:
        return datetime.now() - self.timestamp > timedelta(hours=ttl_hours)

description_cache: Dict[str, CacheEntry] = {}
cache_lock = Lock()

# Активные WebSocket соединения
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_message(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

manager = ConnectionManager()

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
async def increment_request_counter(use_cache=False):
    global request_counter, cached_requests_counter, last_request_time
    
    with counter_lock:
        if use_cache:
            cached_requests_counter += 1
            return
        else:
            request_counter += 1
            current_time = time.time()
            
            with rate_limit_lock:
                time_since_last_request = current_time - last_request_time
                if time_since_last_request < (1.0 / MAX_REQUESTS_PER_SECOND):
                    sleep_time = (1.0 / MAX_REQUESTS_PER_SECOND) - time_since_last_request
                    await asyncio.sleep(sleep_time)
                
                last_request_time = time.time()

def get_request_count():
    with counter_lock:
        return request_counter, cached_requests_counter

def reset_request_counters():
    global request_counter, cached_requests_counter
    with counter_lock:
        request_counter = 0
        cached_requests_counter = 0

async def get_vacancy_description_cached(vacancy_id: str) -> str:
    """Получение описания вакансии с проверкой актуальности кэша"""
    with cache_lock:
        if vacancy_id in description_cache:
            cache_entry = description_cache[vacancy_id]
            if not cache_entry.is_expired():
                await increment_request_counter(use_cache=True)
                return cache_entry.data
            else:
                # Удаляем устаревшую запись
                del description_cache[vacancy_id]
    
    try:
        url = f"{HH_API_BASE_URL}/vacancies/{vacancy_id}"
        response = await asyncio.to_thread(functools.partial(requests.get, url, timeout=10))
        await increment_request_counter(use_cache=False)
        response.raise_for_status()
        data = response.json()
        description = data.get('description', '')
        
        with cache_lock:
            description_cache[vacancy_id] = CacheEntry(description, datetime.now())
        
        return description
    except:
        with cache_lock:
            description_cache[vacancy_id] = CacheEntry("", datetime.now())
        return ""

async def fetch_single_page(search_text: str, area: int, per_page: int, page: int) -> List[Dict]:
    params = {
        'text': search_text,
        'search_field': 'name',
        'area': area,
        'per_page': per_page,
        'page': page,
        'only_with_salary': False
    }
    
    response = await asyncio.to_thread(functools.partial(requests.get, f"{HH_API_BASE_URL}/vacancies", params=params, timeout=10))
    await increment_request_counter(use_cache=False)
    response.raise_for_status()
    data = response.json()
    await asyncio.sleep(REQUEST_DELAY)
    
    return data.get('items', [])

async def get_vacancies_with_progress(search_text: str, area: int, max_pages: int, websocket: WebSocket) -> List[Dict]:
    """Получение вакансий с отправкой прогресса через WebSocket"""
    reset_request_counters()
    
    print(f"Начинаем поиск вакансий: {search_text}, область: {area}, макс. страниц: {max_pages}")
    
    await manager.send_message({
        "stage": "fetching_vacancies",
        "message": "Получаем список вакансий...",
        "progress": 0
    }, websocket)
    
    params = {
        'text': search_text,
        'search_field': 'name',
        'area': area,
        'per_page': 100,
        'page': 0,
        'only_with_salary': False
    }
    
    try:
        response = await asyncio.to_thread(functools.partial(requests.get, f"{HH_API_BASE_URL}/vacancies", params=params, timeout=10))
        await increment_request_counter(use_cache=False)
        response.raise_for_status()
        data = response.json()
        
        total_pages = min(data.get('pages', 1), max_pages)
        found = data.get('found', 0)
        vacancies = data.get('items', [])
        
        print(f"Получен первый запрос: найдено {found} вакансий, страниц: {total_pages}, загружено: {len(vacancies)}")
        
        await manager.send_message({
            "stage": "fetching_vacancies",
            "message": f"Найдено {found} вакансий на {total_pages} страницах",
            "progress": 10,
            "found": found,
            "pages": total_pages,
            "real_requests": 1,
            "cached_requests": 0
        }, websocket)
        
        if total_pages > 1:
            completed_pages = 1
            pages_to_load = list(range(1, total_pages))
            
            # Загружаем страницы последовательно с прогрессом
            for page in pages_to_load:
                try:
                    page_vacancies = await fetch_single_page(search_text, area, 100, page)
                    vacancies.extend(page_vacancies)
                    completed_pages += 1
                    
                    progress = 10 + (40 * completed_pages / total_pages)
                    real_requests, cached_requests = get_request_count()
                    
                    print(f"Загружена страница {completed_pages}/{total_pages}, всего вакансий: {len(vacancies)}")
                    
                    await manager.send_message({
                        "stage": "fetching_vacancies",
                        "message": f"Загружено страниц: {completed_pages}/{total_pages}, всего вакансий: {len(vacancies)}",
                        "progress": int(progress),
                        "completed_pages": completed_pages,
                        "total_pages": total_pages,
                        "loaded_vacancies": len(vacancies),
                        "real_requests": real_requests,
                        "cached_requests": cached_requests
                    }, websocket)
                    
                except Exception as e:
                    print(f"Ошибка при загрузке страницы {page}: {e}")
        
        await manager.send_message({
            "stage": "vacancies_loaded",
            "message": f"Загружено {len(vacancies)} вакансий. Начинаем анализ...",
            "progress": 50,
            "total_vacancies": len(vacancies)
        }, websocket)
        
        print(f"Всего загружено вакансий: {len(vacancies)}, начинаем анализ")
        
        return vacancies
        
    except Exception as e:
        print(f"Ошибка при получении вакансий: {e}")
        import traceback
        traceback.print_exc()
        await manager.send_message({
            "stage": "error",
            "message": f"Ошибка при получении вакансий: {str(e)}",
            "progress": 0
        }, websocket)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении вакансий: {str(e)}")

async def check_vacancy_for_tech(vacancy: Dict, tech_pattern) -> Dict:
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
    if not vacancy_id:
        return {'has_tech': False, 'vacancy_info': None}
    description = (await get_vacancy_description_cached(vacancy_id)).lower()
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

async def analyze_vacancies_with_progress(vacancies: List[Dict], technology: str, websocket: WebSocket) -> Dict:
    """Анализ вакансий с отправкой прогресса"""
    tech_pattern = re.compile(r'\b' + re.escape(technology.lower()) + r'\b', re.IGNORECASE)
    tech_vacancies_details = []
    
    total_vacancies = len(vacancies)
    processed = 0
    
    print(f"Начинаем анализ {total_vacancies} вакансий на технологию: {technology}")
    
    await manager.send_message({
        "stage": "analyzing",
        "message": f"Анализируем вакансии на наличие технологии '{technology}'...",
        "progress": 50,
        "total": total_vacancies,
        "processed": 0,
        "found_with_tech": 0,
        "real_requests": 0,
        "cached_requests": 0
    }, websocket)
    
    # Анализируем вакансии последовательно для корректной отправки прогресса
    for vacancy in vacancies:
        try:
            result = await check_vacancy_for_tech(vacancy, tech_pattern)
            if result['has_tech']:
                tech_vacancies_details.append(result['vacancy_info'])
            
            processed += 1
            
            # Отправляем прогресс каждые 10 вакансий или на последней
            if processed % 10 == 0 or processed == total_vacancies:
                progress = 50 + (45 * processed / total_vacancies)
                real_requests, cached_requests = get_request_count()
                cache_hit_rate = (cached_requests / (real_requests + cached_requests) * 100) if (real_requests + cached_requests) > 0 else 0
                
                print(f"Обработано {processed}/{total_vacancies} вакансий. Запросы: {real_requests} реальных, {cached_requests} кэшированных. Кэш: {cache_hit_rate:.1f}% попаданий")
                
                await manager.send_message({
                    "stage": "analyzing",
                    "message": f"Обработано вакансий: {processed}/{total_vacancies}, найдено с технологией: {len(tech_vacancies_details)}",
                    "progress": int(progress),
                    "processed": processed,
                    "total": total_vacancies,
                    "found_with_tech": len(tech_vacancies_details),
                    "real_requests": real_requests,
                    "cached_requests": cached_requests,
                    "cache_hit_rate": round(cache_hit_rate, 1)
                }, websocket)
                
        except Exception as e:
            print(f"Ошибка при анализе вакансии {vacancy.get('id', 'unknown')}: {e}")
            processed += 1
    
    tech_vacancies = len(tech_vacancies_details)
    
    await manager.send_message({
        "stage": "completed",
        "message": "Анализ завершен!",
        "progress": 100,
        "tech_vacancies": tech_vacancies,
        "total_vacancies": total_vacancies
    }, websocket)
    
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
        "description": "API для анализа технологий в вакансиях с HH.ru",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    # Очищаем устаревшие записи из кэша
    with cache_lock:
        expired_keys = [k for k, v in description_cache.items() if v.is_expired()]
        for key in expired_keys:
            del description_cache[key]
        
        cache_size = len(description_cache)
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "cache_size": cache_size,
        "cache_ttl_hours": CACHE_TTL_HOURS
    }

@app.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    """WebSocket endpoint для анализа с прогрессом в реальном времени"""
    await manager.connect(websocket)
    try:
        # Получаем параметры запроса
        data = await websocket.receive_json()
        
        vacancy_title = data.get('vacancy_title')
        technology = data.get('technology')
        exact_search = data.get('exact_search', True)
        area = data.get('area', 113)
        max_pages = data.get('max_pages', 10)
        
        # Формируем поисковый запрос
        if exact_search:
            search_query = f'"{vacancy_title}"'
        else:
            search_query = vacancy_title
        
        try:
            # Получаем вакансии с прогрессом
            vacancies = await get_vacancies_with_progress(search_query, area, max_pages, websocket)
            
            if not vacancies:
                await manager.send_message({
                    "stage": "error",
                    "message": "Вакансии не найдены",
                    "progress": 0
                }, websocket)
                return
            
            # Анализируем вакансии с прогрессом
            stats = await analyze_vacancies_with_progress(vacancies, technology, websocket)
            
            # Отправляем финальный результат
            real_requests, cached_requests = get_request_count()
            
            result = {
                "stage": "finished",
                "data": {
                    "vacancy_title": vacancy_title,
                    "technology": technology,
                    "total_vacancies": stats['total_vacancies'],
                    "tech_vacancies": stats['tech_vacancies'],
                    "tech_percentage": round(stats['tech_percentage'], 2),
                    "vacancies_with_tech": stats['tech_vacancies_details'],
                    "analysis_timestamp": datetime.now().isoformat(),
                    "request_stats": {
                        "real_requests": real_requests,
                        "cached_requests": cached_requests,
                        "total_requests": real_requests + cached_requests,
                        "cache_size": len(description_cache),
                        "cache_hit_rate": round((cached_requests / (real_requests + cached_requests) * 100) if (real_requests + cached_requests) > 0 else 0, 1)
                    }
                },
                "progress": 100
            }
            
            await manager.send_message(result, websocket)
            
        except Exception as e:
            print(f"Ошибка при анализе: {e}")
            import traceback
            traceback.print_exc()
            await manager.send_message({
                "stage": "error",
                "message": f"Ошибка при анализе: {str(e)}",
                "progress": 0
            }, websocket)
        
    except WebSocketDisconnect:
        print("WebSocket отключен пользователем")
        manager.disconnect(websocket)
    except Exception as e:
        print(f"Ошибка WebSocket: {e}")
        import traceback
        traceback.print_exc()
        try:
            await manager.send_message({
                "stage": "error",
                "message": f"Ошибка сервера: {str(e)}",
                "progress": 0
            }, websocket)
        except:
            pass
    finally:
        manager.disconnect(websocket)

@app.get("/cache/stats")
async def get_cache_statistics():
    """Получить статистику кэша"""
    real_requests, cached_requests = get_request_count()
    total = real_requests + cached_requests
    
    with cache_lock:
        # Очищаем устаревшие записи
        expired_keys = [k for k, v in description_cache.items() if v.is_expired()]
        for key in expired_keys:
            del description_cache[key]
        
        cache_size = len(description_cache)
        
        # Считаем сколько записей истекут в ближайшее время
        expiring_soon = sum(1 for v in description_cache.values() 
                          if (datetime.now() - v.timestamp) > timedelta(hours=CACHE_TTL_HOURS - 1))
    
    return {
        "cache_size": cache_size,
        "real_requests": real_requests,
        "cached_requests": cached_requests,
        "total_requests": total,
        "cache_hit_rate": round((cached_requests / total * 100) if total > 0 else 0, 1),
        "cache_ttl_hours": CACHE_TTL_HOURS,
        "expiring_soon": expiring_soon
    }

@app.delete("/cache/clear")
async def clear_cache():
    """Очистить кэш"""
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