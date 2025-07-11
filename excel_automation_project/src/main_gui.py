import sys
from pathlib import Path
import os

if getattr(sys, 'frozen', False):
    application_path = sys._MEIPASS
    os.chdir(os.path.dirname(sys.executable))
else:
    application_path = os.path.dirname(os.path.abspath(__file__))

sys.path.append(application_path)

import sys
import threading
import time
import psutil
import subprocess

from PySide6.QtWidgets import QApplication, QMainWindow, QMessageBox
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtCore import QUrl, QObject, Signal, Slot, QTimer
from PySide6.QtGui import QFont

class WebBridge(QObject):
    appendLog = Signal(str, str)
    updateStatus = Signal(str)
    updateTimer = Signal(str)
    setButtonsState = Signal(bool, bool, bool, bool)
    setMode = Signal(str)
    stopTimer = Signal()

    @Slot(str, str)
    def runProcess(self, script, name):
        self.run_process_requested.emit(script, name)
    @Slot()
    def stopProcess(self):
        self.stop_process_requested.emit()
    @Slot()
    def clearLog(self):
        self.clear_log_requested.emit()
    @Slot()
    def toggleMode(self):
        self.toggle_mode_requested.emit()
    @Slot()
    def closeApp(self):
        self.close_app_requested.emit()

    run_process_requested = Signal(str, str)
    stop_process_requested = Signal()
    clear_log_requested = Signal()
    toggle_mode_requested = Signal()
    close_app_requested = Signal()

class ProcessWorker(threading.Thread):
    def __init__(self, script_path, process_name, bridge):
        super().__init__(daemon=True)
        self.script_path = script_path
        self.process_name = process_name
        self.bridge = bridge
        self.process = None
        self.stop_requested = False
        # new flags to track markers
        self.found_completed = False
        self.found_error = False

    def run(self):
        try:
            self.process = subprocess.Popen(
                [sys.executable, self.script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Read output line by line
            while True:
                if self.stop_requested:
                    break
                line = self.process.stdout.readline()
                if not line and self.process.poll() is not None:
                    break
                if line:
                    lower = line.lower()
                    # detect markers
                    if '_completed_' in lower:
                        self.found_completed = True
                    if '_error_' in lower:
                        self.found_error = True

                    # decide styling tag
                    if 'warning' in lower:
                        tag = 'warning'
                    elif 'error' in lower:
                        tag = 'error'
                    else:
                        tag = 'normal'
                    self.bridge.appendLog.emit(line, tag)

            # finalize
            self.process.stdout.close()
            return_code = self.process.wait()

            if self.stop_requested:
                self.bridge.appendLog.emit("\nPROCESS STOPPED BY USER\n", 'warning')
                self.bridge.updateStatus.emit(f"{self.process_name} stopped by user")
            elif self.found_completed:
                self.bridge.appendLog.emit("\nPROCESS COMPLETED\n", 'success')
                self.bridge.updateStatus.emit("Process completed")
            elif self.found_error:
                self.bridge.appendLog.emit("\nPROCESS ENCOUNTERED AN ERROR\n", 'error_message')
                self.bridge.updateStatus.emit("Process encountered an error")
            else:
                # fallback if no markers found
                if return_code == 0:
                    self.bridge.updateStatus.emit("Process finished without markers")
                else:
                    self.bridge.appendLog.emit(f"\nPROCESS EXITED WITH CODE {return_code}\n", 'error_message')
                    self.bridge.updateStatus.emit("Process encountered an error")

        except Exception as e:
            self.bridge.appendLog.emit(f"\nERROR RUNNING {self.process_name}: {e}\n", 'error')
            self.bridge.updateStatus.emit("Error running process")
        finally:
            # re‑enable buttons & stop timer
            self.bridge.setButtonsState.emit(True, True,True, False)
            self.bridge.stopTimer.emit()

    def stop(self):
        self.stop_requested = True
        if self.process and self.process.poll() is None:
            try:
                parent = psutil.Process(self.process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
                time.sleep(0.5)
                if parent.is_running():
                    parent.kill()
            except Exception as e:
                self.bridge.appendLog.emit(f"\nERROR TERMINATING PROCESS: {e}\n", 'warning')

    def _terminate_tree(self):
        try:
            if self.process:
                proc = psutil.Process(self.process.pid)
                for child in proc.children(recursive=True):
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass
                try:
                    proc.terminate()
                except psutil.NoSuchProcess:
                    pass
                time.sleep(0.5)
                if proc.is_running(): 
                    proc.kill()
        except Exception as e:
            error_msg = f"\nERROR TERMINATING PROCESS: {str(e)}\n"
            self.bridge.appendLog.emit(error_msg, 'warning')

class WebApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Pharma Data Processing Suite")
        self.resize(1200, 800)

        # Create web view
        self.view = QWebEngineView(self)
        self.setCentralWidget(self.view)
        
        # Create bridge
        self.bridge = WebBridge()

        self.bridge.setButtonsState.emit(True, True, True, False)
        # Connect bridge signals
        self.bridge.run_process_requested.connect(self.run_process)
        self.bridge.stop_process_requested.connect(self.stop_process)
        self.bridge.clear_log_requested.connect(self.clear_log)
        self.bridge.toggle_mode_requested.connect(self.toggle_mode)
        self.bridge.close_app_requested.connect(self.close_app)
        
        # Setup web channel
        self.channel = QWebChannel()
        self.channel.registerObject('bridge', self.bridge)
        self.view.page().setWebChannel(self.channel)
        
        # State management
        self.worker = None
        self.running_process = None
        self.start_time = None
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_timer)
        self.mode = 'light'

        self.log_path = "errors.log"
        
        # Load HTML file
        self.load_html_file()
        
        # Start log tailing
        threading.Thread(target=self.tail_log, daemon=True).start()

    def load_html_file(self):
        if getattr(sys, 'frozen', False):
            # Running in PyInstaller bundle
            html_file = os.path.join(sys._MEIPASS, 'index.html')
        else:
            # Running as normal script
            html_file = Path(__file__).parent / "index.html"
        
        if Path(html_file).exists():
            self.view.load(QUrl.fromLocalFile(html_file))
        else:
            # Fallback: create a minimal HTML interface
            print("HTML file not found. Creating minimal interface.")
            self.view.setHtml("""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Pharma Data Processing Suite</title>
                    <script src="qrc:///qtwebchannel/qwebchannel.js"></script>
                    <style>
                        body { font-family: Arial, sans-serif; padding: 20px; }
                        .error { color: red; font-weight: bold; }
                        button { padding: 10px; margin: 5px; }
                    </style>
                </head>
                <body>
                    <h1>Pharma Data Processing Suite</h1>
                    <p class="error">HTML file not found! Using fallback interface.</p>
                    <div>
                        <button id="btn-1c">Clean 1c File</button>
                        <button id="btn-vtendia">Run Vtendia</button>
                        <button id="btn-etl">Run ETL</button>
                        <button id="btn-stop" disabled>Stop Process</button>
                        <button id="btn-clear">Clear Log</button>
                        <button id="btn-mode">Switch to Dark Mode</button>
                    </div>
                    <div id="status-label">Ready</div>
                    <div id="timer-label">00:00:00</div>
                    <pre id="log-text"></pre>
                    <button id="close-btn">Close</button>
                    
                    <script>
                        document.addEventListener('DOMContentLoaded', () => {
                            new QWebChannel(qt.webChannelTransport, channel => {
                                const bridge = channel.objects.bridge;
                                
                                // Connect UI elements
                                document.getElementById('btn-vtendia').addEventListener('click', () => {
                                    bridge.runProcess('excel_automation/main.py', 'Vtendia');
                                });
                                
                                document.getElementById('btn-etl').addEventListener('click', () => {
                                    bridge.runProcess('dashboard_automation/main2.py', 'ETL');
                                });
                                
                                document.getElementById('btn-stop').addEventListener('click', () => {
                                    bridge.stopProcess();
                                });
                                
                                document.getElementById('btn-clear').addEventListener('click', () => {
                                    bridge.clearLog();
                                });
                                
                                document.getElementById('btn-mode').addEventListener('click', () => {
                                    bridge.toggleMode();
                                });
                                
                                document.getElementById('close-btn').addEventListener('click', () => {
                                    bridge.closeApp();
                                });
                                document.getElementById('btn-1c').addEventListener('click', () => {
                                    bridge.runProcess('cleaning_1c/1c_main.py', 'Clean 1c File');
                                });
                                
                                // Handle signals from Python
                                bridge.appendLog.connect((msg, type) => {
                                    const logText = document.getElementById('log-text');
                                    logText.textContent += msg;
                                });
                                
                                bridge.updateStatus.connect(status => {
                                    document.getElementById('status-label').textContent = status;
                                });
                                
                                bridge.updateTimer.connect(timer => {
                                    document.getElementById('timer-label').textContent = timer;
                                });
                                
                                bridge.setButtonsState.connect((vtendia, etl, oneC, stop) => {
                                    document.getElementById('btn-vtendia').disabled = !vtendia;
                                    document.getElementById('btn-etl').disabled = !etl;
                                    document.getElementById('btn-1c').disabled = !oneC;
                                    document.getElementById('btn-stop').disabled = !stop;
                                });
                            });
                        });
                    </script>
                </body>
                </html>
            """)

    def run_process(self, script, name):
        if self.worker and self.worker.is_alive():
            return
        
        # Clear log
        self.bridge.appendLog.emit("", "clear")
        
        # Update UI state
        self.bridge.setButtonsState.emit(False, False, False, True)
        self.bridge.updateStatus.emit(f"Running {name} process...")
        self.running_process = name
        
        # Start timer
        self.start_time = time.time()
        self.timer.start(1000)  # Update every second

        # Get absolute path to script
        current_dir = Path(__file__).resolve().parent
        project_root = current_dir.parent
        script_path = project_root / script

        # Verify script exists
        if not script_path.exists():
            error_msg = f"ERROR: Script not found at {script_path}\n"
            self.bridge.appendLog.emit(error_msg, 'error')
            self.bridge.updateStatus.emit("Script not found")
            self.bridge.setButtonsState.emit(True, True, False)
            return
                
        # Start worker thread
        self.worker = ProcessWorker(script_path, name, self.bridge)
        self.worker.start()

    def stop_process(self):
        if self.worker:
            self.bridge.updateStatus.emit("Stopping process...")
            self.bridge.setButtonsState.emit(False, False, False, False)
            self.worker.stop()
            self.timer.stop()
            self.worker = None
            self.start_time = None
            # Don't reset timer, just stop updating
            self.bridge.stopTimer.emit()

    def clear_log(self):
        self.bridge.appendLog.emit("", "clear")

    def toggle_mode(self):
        self.mode = 'dark' if self.mode == 'light' else 'light'
        self.bridge.setMode.emit(self.mode)
    def update_timer(self):
        if self.start_time:
            elapsed = time.time() - self.start_time
            hrs, rem = divmod(elapsed, 3600)
            mins, secs = divmod(rem, 60)
            timer_text = f"{int(hrs):02d}:{int(mins):02d}:{int(secs):02d}"
            self.bridge.updateTimer.emit(timer_text)
        else:
            # Reset timer if not running
            self.bridge.updateTimer.emit("00:00:00")

    def tail_log(self):
        """Tail the log file and append to the text widget"""
        try:
            # Create log file if it doesn't exist
            log_path = Path(self.log_path)
            if not log_path.exists():
                log_path.touch()

            with log_path.open('r', encoding='utf-8') as f:
                f.seek(0, 2)  # 2 is equivalent to os.SEEK_END
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(0.2)
                        continue
                    
                    # Determine color based on content
                    lower_line = line.lower()
                    if 'warning' in lower_line:
                        self.bridge.appendLog.emit(line, 'warning')
                    elif 'error' in lower_line:
                        self.bridge.appendLog.emit(line, 'error')
                    else:
                        self.bridge.appendLog.emit(line, 'normal')
        except Exception as e:
            self.bridge.appendLog.emit(f"\nERROR READING LOG: {e}\n", 'warning')

    def close_app(self):
        if self.worker and self.worker.is_alive():
            # Show confirmation dialog
            confirm = QMessageBox.question(
                self, "Confirm Exit",
                "A process is running. Are you sure you want to exit?",
                QMessageBox.Yes | QMessageBox.No
            )
            if confirm == QMessageBox.Yes:
                self.worker.stop()
                QApplication.quit()
        else:
            QApplication.quit()

    def closeEvent(self, event):
        if self.worker and self.worker.is_alive():
            event.ignore()
            self.close_app()
        else:
            event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    window = WebApp()
    window.show()
    sys.exit(app.exec())