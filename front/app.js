let messageStates = [];
const BACKEND_URL = window.env.BACKEND_URL;
const socket = io(BACKEND_URL);

const SUBSCRIBER_GLOSSARY = {
    'residuos_def': 'Residuos',
    'reclamos_def': 'Reclamos',
    'movilidad_def': 'Movilidad',
    'emergencias_def': 'Emergencias',
    'cultura_def': 'Cultura',
    'analitica_def': 'Analítica',
    'residuos_dlx': 'Residuos',
    'reclamos_dlx': 'Reclamos',
    'movilidad_dlx': 'Movilidad',
    'emergencias_dlx': 'Emergencias',
    'cultura_dlx': 'Cultura',
    'analitica_dlx': 'Analítica'
};

const USER_GLOSSARY = {
    'user_residuos': 'Residuos',
    'user_reclamos': 'Reclamos',
    'user_movilidad': 'Movilidad',
    'user_emergencias': 'Emergencias',
    'user_cultura': 'Cultura',
    'user_analitica': 'Analítica'
};

const PUBLISHER_GLOSSARY = {
    'residuos': 'Residuos',
    'reclamos': 'Reclamos',
    'movilidad': 'Movilidad',
    'emergencias': 'Emergencias',
    'cultura': 'Cultura',
    'analitica': 'Analítica'
};

socket.on("connect", () => {
    console.log("Conectado al monitor de estados EDA!");
    updateFooter();
});
socket.on("disconnect", () => updateFooter());
socket.on("message_state", (messageState) => {
    addMessageState(messageState);
});socket.on("new_log", addMessageState);


async function updateFooter() {
    const pageInfo = document.getElementById('page-info');
    if (!pageInfo) return;

    const now = new Date().toLocaleTimeString('es-AR');
    const connectionStatus = socket.connected ? 'Conectado' : 'Desconectado';
    let totalLogs = 0;
    try {
        const data = await res.json();
        totalLogs = data.total;
    } catch {
        totalLogs = messageStates.length;
    }
    pageInfo.textContent = `${connectionStatus} • ${totalLogs} eventos • Última actualización: ${now}`;
}


function formatSubscriber(subscriberText) {
    if (!subscriberText || subscriberText === 'N/A') return 'N/A';
    
    let cleaned = subscriberText.replace(/[{}]/g, '').trim();
    
    let items = cleaned.split(',').map(item => item.trim());
    
    let formatted = items.map(item => SUBSCRIBER_GLOSSARY[item] || item);
    
    return formatted.join(', ');
}

function formatPublisher(publisherText) {
    if (!publisherText || publisherText === 'N/A') return 'N/A';
    
    let cleaned = publisherText.replace(/[{}]/g, '').trim();
    
    let items = cleaned.split(',').map(item => item.trim());
    
    let formatted = items.map(item => PUBLISHER_GLOSSARY[item] || item);
    
    return formatted.join(', ');
}

function formatUser(userText) {
    if (!userText || userText === 'N/A')
        return 'N/A';

    let cleaned = userText.replace(/[{}]/g, '').trim();

    let parts = cleaned.split('_');
    if (parts[0] === 'user') {
        parts.shift();
    }

    let formatted = parts.map(p => p.charAt(0).toUpperCase() + p.slice(1));

    return formatted.join(' ');
}


function createLogEntry(messageState) {
    console.log("Creando entrada para:", messageState);

    let formattedTimestamp;
    try {
        const date = new Date(messageState.event_ts);
        formattedTimestamp = date.toLocaleString('es-AR');
    } catch (e) {
        formattedTimestamp = messageState.event_ts || 'N/A';
    }

    const shortId = messageState.id?.length > 8 ? messageState.id.substring(0, 8) + '...' : (messageState.id || 'N/A');
    const formattedSubscriber = formatSubscriber(messageState.subscriber);
    const formattedPublisher = formatPublisher(messageState.publisher)
    const formattedUser = formatUser(messageState.user);

    return `
    <div class="log-entry" data-status="${messageState.state}" data-topic="${messageState.routing_keys}" data-id="${messageState.id}">
        <div>${shortId}</div>                                 
        <div>${formattedTimestamp}</div>                      
        <div>${formattedUser}</div>             
        <div>${messageState.state || 'N/A'}</div>            
        <div>${messageState.routing_keys || 'N/A'}</div>     
        <div>${formattedPublisher}</div>         
        <div>${formattedSubscriber}</div>       
        <div>${messageState.exchange_name || 'N/A'}</div>    
        <div>${messageState.node || 'N/A'}</div>             
    </div>`;
}

function renderInitialLogs(logs) {
    messageStates = logs.slice();
    const logsList = document.getElementById('logs-list');
    const emptyState = document.getElementById('empty-state');
    if (!logsList) return;

    logsList.innerHTML = "";

    logs.forEach(log => {
        logsList.insertAdjacentHTML('beforeend', createLogEntry(log));
    });

    if (logs.length > 0 && emptyState) {
        emptyState.style.display = 'none';
    }
    updateFooter();
}


function addMessageState(messageState) {
    const emptyState = document.getElementById("empty-state");

    if (messageStates.find(msg => msg.id === messageState.id)) return;

    const logsList = document.getElementById('logs-list');
    if (!logsList) return;
    
    if (emptyState) {
        emptyState.style.display = "none";
    }

    logsList.insertAdjacentHTML('afterbegin', createLogEntry(messageState));

    messageStates.unshift(messageState);

    while (logsList.children.length > 100) {
        logsList.removeChild(logsList.lastChild);
        messageStates.pop();
    }

    updateFooter();
}


let lastTimestamp = null;
let isInitialLoad = true;

async function fetchMessages() {
    const emptyState = document.getElementById('empty-state');
    
    try {
        const url = `${BACKEND_URL}/messages`;
        const response = await fetch(url);
        const messages = await response.json();
        console.log(messages);

        console.log("Mensajes recibidos:", messages.length);

        if (messages.length === 0) {
            console.log("Base de datos vacía");
            
            const logsList = document.getElementById('logs-list');
            
            if (logsList) {
                logsList.innerHTML = '';
            }
            if (emptyState) {
                emptyState.style.display = 'flex';
            }
            
            messageStates = [];
            lastTimestamp = null;
            isInitialLoad = true;
            updateFooter();
            
            return;
        }

        if (emptyState) {
            emptyState.style.display = 'none';
        }

        if (isInitialLoad) {
            renderInitialLogs(messages);
            if (messages.length > 0) {
                lastTimestamp = messages[0].event_ts;
            }
            isInitialLoad = false;
            
        } else {
            
            const existingIds = new Set(messageStates.map(state => state.id || state.event_id));
            
            const newMessages = messages.filter(msg => 
                !existingIds.has(msg.id || msg.event_id)
            );
            
            newMessages.reverse().forEach(msg => addMessageState(msg));

            if (newMessages.length > 0) {
                lastTimestamp = messages[0].event_ts;
                console.log(`Agregados ${newMessages.length} mensajes nuevos`);
            }
        }

    } catch (err) {
        console.error("Error al obtener mensajes desde backend:", err);
    }
}

setInterval(fetchMessages, 1000);


function setupFiltering() {
    const input = document.querySelector('.search-input');
    if (!input) return;

    input.addEventListener('input', () => {
        const term = input.value.toLowerCase();
        document.querySelectorAll('.log-entry').forEach(entry => {
            entry.style.display = Array.from(entry.children)
                .some(cell => cell.textContent.toLowerCase().includes(term)) ? 'grid' : 'none';
        });
    });
}

function downloadLogs() {
    const blob = new Blob([JSON.stringify(messageStates, null, 2)], {type: 'application/json'});
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `EDA_logs_${Date.now()}.json`;
    link.click();
    URL.revokeObjectURL(link.href);
}

document.addEventListener('DOMContentLoaded', () => {
    fetchMessages();
    setupFiltering();
    setInterval(updateFooter, 500);
});

let searchResults = [];

function openSearchModal() {
    document.getElementById('search-modal').style.display = 'block';
}

function closeSearchModal() {
    document.getElementById('search-modal').style.display = 'none';
}

function clearSearchForm() {
    document.getElementById('search-id').value = '';
    document.getElementById('search-user').value = '';
    document.getElementById('search-app-id').value = '';
    document.getElementById('search-state').value = '';
    document.getElementById('search-routing-key').value = '';
    document.getElementById('search-publisher').value = '';
    document.getElementById('search-subscriber').value = '';
    document.getElementById('search-exchange').value = '';
    document.getElementById('search-node').value = '';
    document.getElementById('search-date-from').value = '';
    document.getElementById('search-date-to').value = '';
    document.getElementById('search-results').innerHTML = '';
    document.getElementById('search-results-header').style.display = 'none';
    document.getElementById('download-search-btn').style.display = 'none';
    searchResults = [];
}

async function searchInDatabase() {
    const searchParams = {
        id: document.getElementById('search-id').value.trim(),
        user: document.getElementById('search-user').value.trim(),
        app_id: document.getElementById('search-app-id').value.trim(),
        state: document.getElementById('search-state').value.trim(),
        routing_keys: document.getElementById('search-routing-key').value.trim(),
        publisher: document.getElementById('search-publisher').value.trim(),
        subscriber: document.getElementById('search-subscriber').value.trim(),
        exchange_name: document.getElementById('search-exchange').value.trim(),
        node: document.getElementById('search-node').value.trim(),
        date_from: document.getElementById('search-date-from').value,
        date_to: document.getElementById('search-date-to').value
    };

    console.log('🔎 Parámetros de búsqueda:', searchParams);

    const queryParams = new URLSearchParams();
    Object.keys(searchParams).forEach(key => {
        if (searchParams[key]) {
            queryParams.append(key, searchParams[key]);
        }
    });

    if (queryParams.toString() === '') {
        alert('Por favor, ingresá al menos un criterio de búsqueda');
        return;
    }

    console.log('URL de búsqueda:', `${BACKEND_URL}/search?${queryParams.toString()}`);

    try {
        const response = await fetch(`${BACKEND_URL}/search?${queryParams.toString()}`);
        
        console.log('📡 Response status:', response.status);
        
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Error del servidor:', errorData);
            alert(`Error del servidor: ${errorData.details || errorData.error}`);
            return;
        }

        const data = await response.json();
        console.log('Datos recibidos:', data.length, 'resultados');

        searchResults = data;
        displaySearchResults(data);
    } catch (err) {
        console.error('Error al buscar en la base de datos:', err);
        alert(`Error al buscar: ${err.message}`);
    }
}

function displaySearchResults(results) {
    const resultsDiv = document.getElementById('search-results');
    const resultsHeader = document.getElementById('search-results-header');
    const downloadBtn = document.getElementById('download-search-btn');

    if (results.length === 0) {
        resultsDiv.innerHTML = `
            <div class="search-empty-state">
                <h4>No se encontraron resultados</h4>
                <p>Intentá con otros criterios de búsqueda</p>
            </div>
        `;
        resultsHeader.style.display = 'none';
        downloadBtn.style.display = 'none';
        return;
    }

    resultsHeader.style.display = 'flex';
    downloadBtn.style.display = 'inline-block';

    resultsDiv.innerHTML = `
        <div id="search-results-table-header">
            <div>ID del mensaje</div>
            <div>Timestamp</div>
            <div>Usuario</div>
            <div>ID de App</div>
            <div>Estado</div>
            <div>Routing_key</div>
            <div>Publicador</div>
            <div>Suscriptor</div>
            <div>Exchange</div>
            <div>Nodo</div>
        </div>
        <div id="search-results-list">
            ${results.map(log => createLogEntry(log)).join('')}
        </div>
    `;
}

function downloadSearchResults() {
    if (searchResults.length === 0) {
        alert('No hay resultados para descargar');
        return;
    }

    const blob = new Blob([JSON.stringify(searchResults, null, 2)], {type: 'application/json'});
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `EDA_search_results_${Date.now()}.json`;
    link.click();
    URL.revokeObjectURL(link.href);
}

window.onclick = function(event) {
    const modal = document.getElementById('search-modal');
    if (event.target === modal) {
        closeSearchModal();
    }
}