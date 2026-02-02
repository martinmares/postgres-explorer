(() => {
  const basePath = window.basePath || '';
  window.currentStep = 1;
  let jobId = null;
  let eventSource = null;
  let startTime = null;
  let durationInterval = null;
  let uploadedFilePath = null;
  let uploadedFormat = null;

  const getEl = (id) => document.getElementById(id);

  function resetImportWizard() {
    if (!getEl('import-wizard-root')) return;

    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
    if (durationInterval) {
      clearInterval(durationInterval);
      durationInterval = null;
    }

    window.currentStep = 1;
    jobId = null;
    startTime = null;
    uploadedFilePath = null;
    uploadedFormat = null;

    const fileInput = getEl('file-input');
    if (fileInput) fileInput.value = '';

    const uploadProgress = getEl('upload-progress');
    if (uploadProgress) uploadProgress.style.display = 'none';
    const uploadSuccess = getEl('upload-success');
    if (uploadSuccess) uploadSuccess.style.display = 'none';

    const errorAlerts = getEl('upload-success')?.parentElement?.querySelectorAll('.alert.alert-danger') || [];
    errorAlerts.forEach((el) => el.remove());

    const importStatus = getEl('import-status');
    if (importStatus) importStatus.style.display = 'none';
    const terminal = getEl('terminal-output-import');
    if (terminal) terminal.innerHTML = '';
    const jobStatus = getEl('job-status-import');
    if (jobStatus) {
      jobStatus.textContent = 'Running...';
      jobStatus.style.color = '';
    }
    const jobDuration = getEl('job-duration-import');
    if (jobDuration) jobDuration.textContent = '00:00';

    const btnExecute = getEl('btn-execute-import');
    if (btnExecute) btnExecute.disabled = false;
    const btnNext = getEl('btn-next-import');
    if (btnNext) {
      btnNext.disabled = true;
      btnNext.setAttribute('disabled', 'disabled');
    }

    updateStepImport();
  }

  async function handleFileUpload(file) {
    getEl('upload-progress').style.display = 'block';
    getEl('upload-success').style.display = 'none';
    const formData = new FormData();
    formData.append('file', file);
    try {
      const response = await fetch(`${basePath}/maintenance/import/upload`, { method: 'POST', body: formData });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text}`);
      }
      const data = await response.json();
      uploadedFilePath = data.file_path;
      uploadedFormat = data.format;
      getEl('upload-progress').style.display = 'none';
      getEl('upload-success').style.display = 'block';
      getEl('uploaded-filename').textContent = file.name;
      getEl('uploaded-size').textContent = (data.file_size / 1024 / 1024).toFixed(2) + ' MB (' + data.format + ')';
      const btnNext = getEl('btn-next-import');
      if (btnNext) {
        btnNext.disabled = false;
        btnNext.removeAttribute('disabled');
      }
    } catch (err) {
      getEl('upload-progress').style.display = 'none';
      const errorDiv = document.createElement('div');
      errorDiv.className = 'alert alert-danger mt-3';
      errorDiv.innerHTML = '<i class="ti ti-alert-circle me-2"></i><strong>Upload failed:</strong> ' + err.message;
      getEl('upload-success').parentElement.appendChild(errorDiv);
      console.error('Upload error:', err);
    }
  }

  function updateStepImport() {
    for (let i = 1; i <= 4; i++) {
      getEl(`step-${i}`).style.display = 'none';
      getEl(`step-${i}-tab`).classList.remove('active');
    }
    getEl(`step-${window.currentStep}`).style.display = 'block';
    getEl(`step-${window.currentStep}-tab`).classList.add('active');
    getEl('btn-prev-import').disabled = window.currentStep === 1;
    getEl('btn-next-import').style.display = window.currentStep === 4 ? 'none' : 'inline-block';
    getEl('btn-execute-import').style.display = window.currentStep === 4 ? 'inline-block' : 'none';
    if (window.currentStep === 4) updateCommandPreviewImport();
  }

  function updateCommandPreviewImport() {
    const isPlain = uploadedFormat === 'plain';
    const db = getEl('target-database').value || '[database]';

    if (isPlain) {
      let cmd = 'PGPASSWORD=***** psql -h [host] -p [port] -U [user] -d ' + db;
      if (getEl('single-transaction').checked) cmd += ' --single-transaction';
      cmd += ' -f [file]';
      getEl('command-preview-import').textContent = cmd;
    } else {
      const createDb = getEl('create-db').checked;
      let cmd = 'PGPASSWORD=***** pg_restore -h [host] -p [port] -U [user]';

      if (createDb) {
        cmd += ' -d [maintenance_db] --dbname ' + db;
      } else {
        cmd += ' -d ' + db;
      }

      if (getEl('clean').checked) cmd += ' --clean';
      if (createDb) cmd += ' --create';
      if (getEl('data-only').checked) cmd += ' --data-only';
      if (getEl('schema-only').checked) cmd += ' --schema-only';
      if (getEl('disable-triggers').checked) cmd += ' --disable-triggers';
      if (getEl('single-transaction').checked) cmd += ' --single-transaction';
      if (getEl('verbose-import').checked) cmd += ' --verbose';
      cmd += ' --no-owner [file]';
      getEl('command-preview-import').textContent = cmd;
    }
  }

  async function startImport() {
    if (!uploadedFilePath) { alert('No file uploaded'); return; }
    const payload = {
      file_path: uploadedFilePath,
      format: uploadedFormat,
      target_database: getEl('target-database').value,
      clean: getEl('clean').checked,
      create_db: getEl('create-db').checked,
      data_only: getEl('data-only').checked,
      schema_only: getEl('schema-only').checked,
      disable_triggers: getEl('disable-triggers').checked,
      single_transaction: getEl('single-transaction').checked,
      verbose: getEl('verbose-import').checked,
      pg_version: 'auto'
    };
    getEl('btn-execute-import').disabled = true;
    getEl('import-status').style.display = 'block';
    try {
      const response = await fetch(`${basePath}/maintenance/import`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const data = await response.json();
      jobId = data.job_id;
      startLogStreamImport(jobId);
      startDurationTimerImport();
    } catch (err) {
      appendTerminalImport(`ERROR: ${err.message}`, 'red');
      getEl('job-status-import').textContent = 'Failed';
    }
  }

  function startLogStreamImport(jobIdLocal) {
    eventSource = new EventSource(`${basePath}/maintenance/import/${jobIdLocal}/logs`);
    eventSource.onmessage = (event) => appendTerminalImport(event.data);
    eventSource.onerror = () => { eventSource.close(); checkJobStatusImport(jobIdLocal); };
  }

  async function checkJobStatusImport(jobIdLocal) {
    try {
      const response = await fetch(`${basePath}/maintenance/import/${jobIdLocal}/status`);
      const data = await response.json();
      const statusEl = getEl('job-status-import');
      statusEl.textContent = data.status;
      if (data.status === 'Completed') { if (durationInterval) clearInterval(durationInterval); statusEl.style.color = '#27c93f'; }
      else if (data.status === 'Failed') { if (durationInterval) clearInterval(durationInterval); statusEl.style.color = '#ff5f56'; if (data.error) appendTerminalImport('FAILED: ' + data.error, '#ff5f56'); }
    } catch (err) { console.error('Failed to check status:', err); }
  }

  function appendTerminalImport(text, color = '#0f0') {
    const terminal = getEl('terminal-output-import');
    const line = document.createElement('div');
    line.style.color = color;
    line.textContent = text;
    terminal.appendChild(line);
    terminal.scrollTop = terminal.scrollHeight;
  }

  function startDurationTimerImport() {
    startTime = Date.now();
    durationInterval = setInterval(() => {
      const elapsed = Math.floor((Date.now() - startTime) / 1000);
      getEl('job-duration-import').textContent = Math.floor(elapsed / 60).toString().padStart(2, '0') + ':' + (elapsed % 60).toString().padStart(2, '0');
    }, 1000);
  }

  function copyLogsImport(btn) {
    const terminal = getEl('terminal-output-import');
    const text = Array.from(terminal.children).map(line => line.textContent).join('\n');
    navigator.clipboard.writeText(text).then(() => {
      btn.setAttribute('title', 'Copied!');
      const tooltip = new bootstrap.Tooltip(btn, { trigger: 'manual' });
      tooltip.show();
      setTimeout(() => { tooltip.dispose(); btn.setAttribute('title', ''); }, 2000);
    }).catch(err => console.error('Failed to copy:', err));
  }

  function saveLogsImport() {
    if (!jobId) {
      console.error('No job ID available for log download');
      return;
    }
    // Download full log file from server (not just UI visible part)
    const a = document.createElement('a');
    a.href = `${basePath}/maintenance/import/${jobId}/download-log`;
    a.download = `${jobId}.log`;
    a.click();
    URL.revokeObjectURL(url);
  }

  function initImportWizard() {
    if (!getEl('import-wizard-root')) return;
    const btnNext = getEl('btn-next-import');
    if (btnNext) {
      btnNext.replaceWith(btnNext.cloneNode(true));
    }
    const btnPrev = getEl('btn-prev-import');
    if (btnPrev) {
      btnPrev.replaceWith(btnPrev.cloneNode(true));
    }
    resetImportWizard();
    const btnNextFresh = getEl('btn-next-import');
    if (btnNextFresh) {
      btnNextFresh.addEventListener('click', () => {
        if (window.currentStep < 4) {
          window.currentStep += 1;
          updateStepImport();
        }
      });
    }
    const btnPrevFresh = getEl('btn-prev-import');
    if (btnPrevFresh) {
      btnPrevFresh.addEventListener('click', () => {
        if (window.currentStep > 1) {
          window.currentStep -= 1;
          updateStepImport();
        }
      });
    }
  }

  window.initImportWizard = initImportWizard;
  window.handleFileUpload = handleFileUpload;
  window.updateStepImport = updateStepImport;
  window.updateCommandPreviewImport = updateCommandPreviewImport;
  window.startImport = startImport;
  window.copyLogsImport = copyLogsImport;
  window.saveLogsImport = saveLogsImport;

})();
