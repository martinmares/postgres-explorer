(function () {
  function resetEndpointForm() {
    const form = document.getElementById('endpoint-form');
    const title = document.getElementById('endpoint-modal-title');
    const submitBtn = document.getElementById('endpoint-submit-btn');
    if (!form || !title || !submitBtn) return;

    form.reset();
    form.setAttribute('hx-post', '/endpoints');
    form.removeAttribute('hx-put');
    if (window.htmx) {
      window.htmx.process(form);
    }
    // Explicitně nastav výchozí hodnoty pro nové formuláře
    form.querySelector('[name="ssl_mode"]').value = '';
    form.querySelector('[name="search_path"]').value = '';
    title.textContent = 'Add Postgres Connection';
    submitBtn.innerHTML = '<i class="ti ti-check"></i> Save Connection';
  }

  function openEditEndpoint(button) {
    const form = document.getElementById('endpoint-form');
    const title = document.getElementById('endpoint-modal-title');
    const submitBtn = document.getElementById('endpoint-submit-btn');
    if (!form || !title || !submitBtn) return;

    const id = button.dataset.endpointId;
    form.setAttribute('hx-put', `/endpoints/${id}`);
    form.removeAttribute('hx-post');
    if (window.htmx) {
      window.htmx.process(form);
    }
    form.querySelector('[name="name"]').value = button.dataset.endpointName || '';
    form.querySelector('[name="url"]').value = button.dataset.endpointUrl || '';
    form.querySelector('[name="insecure"]').checked = button.dataset.endpointInsecure === 'true';
    form.querySelector('[name="username"]').value = button.dataset.endpointUsername || '';
    form.querySelector('[name="password"]').value = '';
    form.querySelector('[name="ssl_mode"]').value = button.dataset.endpointSslMode || '';
    form.querySelector('[name="search_path"]').value = button.dataset.endpointSearchPath || '';
    form.querySelector('[name="enable_blueprint"]').checked = button.dataset.endpointEnableBlueprint === 'true';
    title.textContent = 'Edit Postgres Connection';
    submitBtn.innerHTML = '<i class="ti ti-check"></i> Update Connection';

    const modal = new bootstrap.Modal(document.getElementById('modal-endpoint'));
    modal.show();
  }

  function confirmDelete(id, name) {
    window.__pgDeleteEndpointId = id;
    const nameEl = document.getElementById('delete-endpoint-name');
    if (nameEl) nameEl.textContent = name;
    const modal = new bootstrap.Modal(document.getElementById('modal-delete'));
    modal.show();
  }

  async function testConnection(evt, id) {
    const btn = evt.target.closest('button');
    const originalHtml = btn.innerHTML;
    const name = btn.dataset.endpointName || '';
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

    try {
      const response = await fetch(`/endpoints/${id}/test`, { method: 'POST' });
      const result = await response.json();

      const toast = document.createElement('div');
      const bgColor = result.success ? '#2fb344' : '#d63939';
      toast.style = `
        position: fixed;
        top: 100px;
        right: 20px;
        z-index: 10000;
        min-width: 320px;
        max-width: 420px;
        padding: 1rem;
        background-color: ${bgColor};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        opacity: 0;
      `;
      toast.innerHTML = `
        <div style="display: flex; align-items: start;">
          <div style="margin-right: 12px; font-size: 24px;">
            ${result.success ? '✓' : '✗'}
          </div>
          <div style="flex: 1; color: white;">
            <h4 style="margin: 0 0 4px 0; font-size: 16px; font-weight: bold; color: white;">${name}</h4>
            <div style="color: rgba(255,255,255,0.9); font-size: 14px;">${result.message}</div>
            ${result.version ? `<div style="margin-top: 4px; font-size: 13px; color: rgba(255,255,255,0.8);"><strong>Version:</strong> ${result.version}</div>` : ''}
          </div>
          <button onclick="this.parentElement.parentElement.remove()" style="background: none; border: none; color: white; font-size: 20px; cursor: pointer; margin-left: 8px;">&times;</button>
        </div>
      `;
      document.body.appendChild(toast);

      setTimeout(() => {
        toast.style.opacity = '1';
        toast.style.transition = 'opacity 0.3s ease-in';
      }, 10);

      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 0.3s ease-out';
        setTimeout(() => toast.remove(), 300);
      }, 5000);
    } catch (error) {
      alert('Error testing connection: ' + error.message);
    } finally {
      btn.disabled = false;
      btn.innerHTML = originalHtml;
    }
  }

  function bindEndpointEvents() {
    function refreshEndpointsList() {
      const base = window.__BASE_PATH__ || '';
      return fetch(`${base}/endpoints`, { method: 'GET' })
        .then(res => res.text())
        .then(pageHtml => {
          const parser = new DOMParser();
          const doc = parser.parseFromString(pageHtml, 'text/html');
          const newList = doc.getElementById('endpoints-list');
          const list = document.getElementById('endpoints-list');
          if (newList && list) {
            list.innerHTML = newList.innerHTML;
            return;
          }
          window.location.href = `${base}/endpoints`;
        })
        .catch(() => {
          window.location.href = `${base}/endpoints`;
        });
    }

    const confirmDeleteBtn = document.getElementById('confirm-delete-btn');
    if (confirmDeleteBtn && !confirmDeleteBtn.dataset.bound) {
      confirmDeleteBtn.dataset.bound = 'true';
      confirmDeleteBtn.addEventListener('click', function () {
        const id = window.__pgDeleteEndpointId;
        if (!id) return;
        const base = window.__BASE_PATH__ || '';
        const url = `${base}/endpoints/${id}`;
        fetch(url, { method: 'DELETE' })
          .then(res => {
            if (!res.ok) {
              throw new Error('Delete failed');
            }
            return refreshEndpointsList();
          })
          .catch(() => {
            window.location.href = `${base}/endpoints`;
          });
        const modalEl = document.getElementById('modal-delete');
        const modal = bootstrap.Modal.getInstance(modalEl);
        if (modal) modal.hide();
      });
    }

    const endpointModal = document.getElementById('modal-endpoint');
    if (endpointModal && !endpointModal.dataset.bound) {
      endpointModal.dataset.bound = 'true';
      endpointModal.addEventListener('hidden.bs.modal', resetEndpointForm);
    }

    if (!document.body.dataset.endpointsAfterSwapBound) {
      document.body.dataset.endpointsAfterSwapBound = 'true';
      document.body.addEventListener('htmx:afterSwap', function (evt) {
        if (evt.detail.target && evt.detail.target.id === 'endpoints-list') {
          const modalElement = document.getElementById('modal-endpoint');
          const modal = bootstrap.Modal.getInstance(modalElement);
          if (modal) {
            modal.hide();
          }

          resetEndpointForm();

          const errorBox = document.getElementById('endpoint-form-error');
          if (errorBox) {
            errorBox.textContent = '';
            errorBox.classList.add('d-none');
          }

          setTimeout(() => {
            const backdrops = document.querySelectorAll('.modal-backdrop');
            backdrops.forEach(backdrop => backdrop.remove());
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
            document.body.style.paddingRight = '';
          }, 100);
        }
      });
    }

    if (!document.body.dataset.endpointsAfterRequestBound) {
      document.body.dataset.endpointsAfterRequestBound = 'true';
      document.body.addEventListener('htmx:afterRequest', function (evt) {
        const form = document.querySelector('#modal-endpoint form');
        if (!form || evt.detail.requestConfig.elt !== form) return;
        if (!evt.detail.xhr || evt.detail.xhr.status >= 400) return;
        refreshEndpointsList()
          .finally(() => {
            const modalElement = document.getElementById('modal-endpoint');
            const modal = bootstrap.Modal.getInstance(modalElement);
            if (modal) modal.hide();
            resetEndpointForm();
          });
      });
    }

    if (!document.body.dataset.endpointsBeforeRequestBound) {
      document.body.dataset.endpointsBeforeRequestBound = 'true';
      document.body.addEventListener('htmx:beforeRequest', function (evt) {
        const form = document.querySelector('#modal-endpoint form');
        if (form && evt.detail.requestConfig.elt === form) {
          const errorBox = document.getElementById('endpoint-form-error');
          if (errorBox) {
            errorBox.textContent = '';
            errorBox.classList.add('d-none');
          }
        }
      });
    }

    if (!document.body.dataset.endpointsResponseErrorBound) {
      document.body.dataset.endpointsResponseErrorBound = 'true';
      document.body.addEventListener('htmx:responseError', function (evt) {
        const form = document.querySelector('#modal-endpoint form');
        if (form && evt.detail.requestConfig.elt === form) {
          const errorBox = document.getElementById('endpoint-form-error');
          if (errorBox) {
            errorBox.textContent = evt.detail.xhr.responseText || 'Failed to save connection.';
            errorBox.classList.remove('d-none');
          }
        }
      });
    }
  }

  window.openEditEndpoint = openEditEndpoint;
  window.confirmDelete = confirmDelete;
  window.testConnection = testConnection;
  window.pgEndpointsInit = bindEndpointEvents;

  document.addEventListener('DOMContentLoaded', bindEndpointEvents);
  document.addEventListener('htmx:afterSwap', bindEndpointEvents);
})();
