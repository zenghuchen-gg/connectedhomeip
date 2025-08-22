import logging
import time
import concurrent.futures
import threading
import serial
import serial.tools.list_ports
import sys
from typing import List, Optional

# The following are imports from Pigweed's Python packages
# These would be installed in the environment using activate.sh
try:
    from pw_hdlc.rpc import HdlcRpcClient, default_channels
    from pw_rpc.client import Client
    from pw_stream import stream_readers
    from pw_rpc.descriptors import Channel, Service, Method, PendingRpc
    from pw_status import Status
    from google.protobuf import empty_pb2
    from device_service import device_service_pb2
    from lighting_service import lighting_service_pb2
    from thread_service import thread_service_pb2
except ImportError:
    print("Error: Pigweed python packages not found.", file=sys.stderr)
    sys.exit(1)

# Set up logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s: %(message)s')
_LOG = logging.getLogger('PigweedRpcDeviceDiscoverer')
_LOG.setLevel(logging.INFO)


# Disable the pw_hdlc.rpc logger to prevent it from printing to stdout.
# This is done to avoid cluttering the console output.
logging.getLogger('pw_hdlc').setLevel(logging.CRITICAL)

class DiscoveredDevice:
    """A simple class to hold information about a discovered device."""

    def __init__(self, port: str, client: Client, rpcs, serial_dev: serial.Serial, reader: stream_readers.SerialReader):
        self.port = port
        self.client = client
        self.rpcs = rpcs
        self.serial_dev = serial_dev
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitor_stop_event: Optional[threading.Event] = None
        self.reader = reader

    def GetState(self, timeout_s: float = 2.0) -> tuple[int, int, int, bool, bool]:
        """Gets state from the device using Pigweed RPCs.

        Calls Device.GetDeviceState, Lighting.Get, and Thread.GetState.

        Returns:
            A tuple containing:
            - node_id (int)
            - total_secure_sessions (int)
            - active_subscriptions (int)
            - light_on (bool)
            - thread_is_attached (bool)

        Raises:
            Exception: If an RPC call fails or times out.
        """
        device_service = self.rpcs.chip.rpc.Device
        lighting_service = self.rpcs.chip.rpc.Lighting
        thread_service = self.rpcs.chip.rpc.Thread

        device_status, device_state = device_service.GetDeviceState(pw_rpc_timeout_s=timeout_s)
        if not device_status.ok():
            raise Exception(f"GetDeviceState RPC failed: {device_status}")

        light_status, light_state = lighting_service.Get(pw_rpc_timeout_s=timeout_s)
        if not light_status.ok():
            raise Exception(f"Lighting.Get RPC failed: {light_status}")

        thread_status, thread_state = thread_service.GetState(pw_rpc_timeout_s=timeout_s)
        if not thread_status.ok():
            raise Exception(f"Thread.GetState RPC failed: {thread_status}")

        # For now, we only care about the first fabric's node ID.
        node_id = device_state.fabric_info[0].node_id if len(device_state.fabric_info) > 0 else None

        return (node_id, device_state.total_secure_sessions, device_state.active_subscriptions, light_state.on, thread_state.is_attached)

    def monitor(self, callback: callable, interval_s: float = 1.0):
        """Starts a thread to periodically poll GetState and emit to a callback.

        Args:
            callback: A callable that receives the device instance and its state tuple.
                      e.g., my_callback(device: DiscoveredDevice, state: tuple)
            interval_s: The polling interval in seconds.
        """
        if self._monitor_thread and self._monitor_thread.is_alive():
            _LOG.warning("Monitor is already running for %s", self.port)
            return

        self._monitor_stop_event = threading.Event()
        self._monitor_thread = threading.Thread(target=self._monitor_loop,
                                                args=(callback, interval_s, self._monitor_stop_event))
        self._monitor_thread.daemon = True  # So it doesn't block program exit
        self._monitor_thread.start()
        _LOG.debug("Started monitoring device on port %s", self.port)

    def _monitor_loop(self, callback: callable, interval_s: float, stop_event: threading.Event):
        while not stop_event.is_set():
            try:
                state = self.GetState()
                callback(self, state)
            except Exception as e:
                _LOG.error("Error while monitoring device %s: %s. Stopping monitor.", self.port, e)
                break  # Exit loop on error
            stop_event.wait(interval_s)

    def stop_monitor(self):
        """Stops the monitoring thread if it is running."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            _LOG.debug("Stopping monitor for device on port %s", self.port)
            self._monitor_stop_event.set()
            self._monitor_thread.join()
            self._monitor_thread = None
            self._monitor_stop_event = None

    def close(self):
        """Closes the connection to the device and stops monitoring."""
        self.stop_monitor()

        if self.client:
            self.client.close()

        if self.serial_dev and self.serial_dev.is_open:
            self.serial_dev.close()

        if self.reader:
            self.reader.__exit__(None, None, None)

class PigweedRpcDeviceDiscoverer:
    """
    Discovers devices running a Pigweed RPC server over serial.
    """

    def __init__(self, baudrate: int = 115200, timeout: float = 1.0, log_callback: Optional[callable] = None):
        """
        Initializes the discoverer.

        Args:
            baudrate: The serial baud rate to use for communication.
            timeout: The timeout in seconds for serial and RPC operations.
            log_callback: A callback to receive log messages from the device.
                          The callback should accept bytes.
        """
        self.baudrate = baudrate
        self.timeout = timeout
        self.log_callback = log_callback
        self._protos = [device_service_pb2, lighting_service_pb2, thread_service_pb2]

    def discover(self, path_prefix: str, timeout_s: Optional[float] = None) -> List[DiscoveredDevice]:
        """
        Discovers Pigweed RPC devices on serial ports matching a prefix.

        It probes each potential port in parallel by trying to establish an RPC
        session and calling a service.

        Args:
            path_prefix: The prefix for serial device files to scan,
                         e.g., '/dev/ttyACM' on Linux or 'COM' on Windows.
            timeout_s: Optional total timeout in seconds for the discovery process.

        Returns:
            A list of DiscoveredDevice objects for each device found.
        """
        discovered_devices: List[DiscoveredDevice] = []
        # Use serial.tools.list_ports for robust cross-platform port listing.
        potential_ports = [
            p.device for p in serial.tools.list_ports.comports()
            if p.device.startswith(path_prefix)
        ]

        if not potential_ports:
            _LOG.warning("No serial ports found matching prefix: %s", path_prefix)
            return []

        _LOG.info("Probing %d potential port(s) with prefix: %s", len(potential_ports), path_prefix)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(potential_ports) or 1) as executor:
            future_to_port = {
                executor.submit(self._probe_device_with_retry, port, retries=2): port
                for port in potential_ports
            }

            done, not_done = concurrent.futures.wait(future_to_port, timeout=timeout_s)

            for future in done:
                port = future_to_port[future]
                try:
                    device = future.result()
                    if device:
                        _LOG.debug("Found Pigweed device on %s", port)
                        discovered_devices.append(device)
                    else:
                        _LOG.debug("No Pigweed device found on %s", port)
                except Exception as exc:
                    _LOG.error("Probe for port %s generated an exception: %s", port, exc)

            if not_done:
                _LOG.warning("Device discovery timed out for %d ports.", len(not_done))
                for future in not_done:
                    future.cancel()

        return discovered_devices

    def _close_probe_resources(self, rpc_client: Optional[HdlcRpcClient],
                               serial_device: Optional[serial.Serial], reader: Optional[stream_readers.SerialReader]):
        """Safely close resources used by a probe on failure."""
        if reader:
            reader.__exit__(None, None, None)
        if rpc_client:
            rpc_client.close()
        time.sleep(0.2)
        if serial_device and serial_device.is_open:
            try:
                serial_device.close()
            except Exception as e:
                _LOG.debug("Error closing serial device: %s", e)
                
    def _probe_device_with_retry(self, port_name: str, retries: int = 3, retry_delay_s: float = 0.5) -> Optional[DiscoveredDevice]:
        """Tries to probe a device, with retries on failure."""
        for attempt in range(retries):
            device = self._probe_device(port_name)
            if device:
                return device
            if attempt < retries - 1:
                _LOG.debug("Probe failed on %s. Retrying in %s s...", port_name, retry_delay_s)
                time.sleep(retry_delay_s)
        return None

    def _probe_device(self, port_name: str) -> Optional[DiscoveredDevice]:
        """
        Tries to connect to a serial port and get device info.

        Returns a DiscoveredDevice object if successful, None otherwise.
        """
        _LOG.debug("Probing port %s...", port_name)
        rpc_client = None
        serial_device = None
        try:
            serial_device = serial.Serial(port_name,
                                          self.baudrate,
                                          timeout=self.timeout)
            reader = stream_readers.SerialReader(serial_device)
        except serial.SerialException as e:
            _LOG.debug("Failed to open serial port %s: %s", port_name, e)
            return None

        if not serial_device.is_open:
            _LOG.debug("Serial port %s is not open.", port_name)
            return None
        _LOG.debug("Serial port %s is open.", port_name)

        try:
            hdlc_client_kwargs = {
                'reader': reader,
                'paths_or_modules': self._protos,
                'channels': default_channels(serial_device.write),
            }
            if self.log_callback:
                hdlc_client_kwargs['output'] = self.log_callback

            rpc_client = HdlcRpcClient(**hdlc_client_kwargs)

            rpcs = rpc_client.client.channel().rpcs
            device_service = rpcs.chip.rpc.Device
            status, response = device_service.GetDeviceInfo(pw_rpc_timeout_s=2.0)

            if status.ok():
                _LOG.debug("Device info from %s: %s", port_name, response)
                return DiscoveredDevice(port_name, rpc_client, rpcs, serial_device, reader)
            else:
                _LOG.debug("Probe failed on %s. Status: %s", port_name, status)
                self._close_probe_resources(rpc_client, serial_device, reader)
                return None

        except Exception as e:
            _LOG.debug("Exception while probing %s: %s", port_name, e)
            self._close_probe_resources(rpc_client, serial_device, reader)
            return None

class LightGrid:
    class AnsiBack:
        """ANSI escape codes for background colors."""
        MAGENTA = '\033[45m'
        CYAN = '\033[46m'
        WHITE = '\033[47m'
        RESET = '\033[0m'

    class AnsiFore:
        """ANSI escape codes for foreground colors."""
        RED = '\033[31m'

    class AnsiControl:
        """ANSI control codes."""
        CURSOR_UP = lambda lines: f'\033[{lines}A'
        ERASE_LINE = '\033[2K'
        HIDE_CURSOR = '\033[?25l'
        SHOW_CURSOR = '\033[?25h'

    # TODO this map should come from a config file.
    _DEVICE_MAP = {
        1016577699: ("A", 0), 1418836604: ("A", 1), 922432105: ("A", 2),
        1953479049: ("A", 3), 2736850227: ("A", 4), 4240688217: ("A", 5),
        1031896585: ("A", 6), 1775774603: ("A", 7), 2554983592: ("A", 8),
        1031850644: ("A", 9), 2045240419: ("A", 10), 3212830052: ("A", 11),
        744351325: ("A", 12), 1195384911: ("A", 13), 2995023597: ("A", 14),
        781991772: ("A", 15), 1939977533: ("B", 0), 3699486543: ("B", 1),
        666054600: ("B", 2), 2875473849: ("B", 3), 2023898678: ("B", 4),
        1249672984: ("B", 5), 2462130576: ("B", 6), 2892028506: ("B", 7),
        1845992409: ("B", 8), 1820720483: ("B", 9), 2137464538: ("B", 10),
        39571803: ("B", 11), 1710697481: ("B", 12), 3610441492: ("B", 13),
        2300088043: ("B", 14), 2368654102: ("B", 15), 1178377283: ("C", 0),
        2749828027: ("C", 1), 3567195770: ("C", 2), 4094801446: ("C", 3),
        4266894574: ("C", 4), 2995339882: ("C", 5), 731863662: ("C", 6),
        697171714: ("C", 7), 1248305403: ("C", 8), 1794630827: ("C", 9),
        3646785438: ("C", 10), 3052279175: ("C", 11), 3426013203: ("C", 12),
        294955643: ("C", 13), 3239097901: ("C", 14), 3807763175: ("C", 15),
        450783804: ("D", 0), 3676721468: ("D", 1), 1308967914: ("D", 2),
        2277391937: ("D", 3), 3444897486: ("D", 4), 2020842874: ("D", 5),
        3918742481: ("D", 6), 3416560164: ("D", 7), 434117947: ("D", 8),
        73320146: ("D", 9), 2929987377: ("D", 10), 119116303: ("D", 11),
        700342186: ("D", 12), 3503994431: ("D", 13), 792219265: ("D", 14),
        1255186498: ("D", 15)
    }

    _NUM_HEADER_LINES = 1
    _NUM_GRID_LINES = 5  # 4 hubs + 1 for unmapped
    _NUM_FOOTER_LINES = 1
    _TOTAL_OUTPUT_LINES = _NUM_HEADER_LINES + _NUM_GRID_LINES + _NUM_FOOTER_LINES

    def __init__(self):
        self._device_states = {}
        for letter in ["A", "B", "C", "D"]:
            self._device_states[letter] = [None] * 16
        self._device_states[None] = {}  # For devices not in the map
        self._first_run = True

    def _colorize(self, char: str, bg_color: str, fg_color: str = "") -> str:
        """Wraps a character with color codes."""
        return f"{fg_color}{bg_color}{char}{self.AnsiBack.RESET}"

    def update_device_state(self, device: DiscoveredDevice, state: tuple):
        """Callback to update the internal state dictionary."""
        node_id, _, _, _, _ = state
        if node_id in self._DEVICE_MAP:
            # TODO Could have a timestamp on the state so we know if we lose comms
            hub, index = self._DEVICE_MAP[node_id]
            self._device_states[hub][index] = state
        else:
            self._device_states[None][device.port] = state

    def render(self, status: str = ""):
        """Displays the device state grid, overwriting previous output."""
        if not self._first_run:
            sys.stdout.write(self.AnsiControl.CURSOR_UP(self._TOTAL_OUTPUT_LINES))

        lines = [f'\r{self.AnsiControl.ERASE_LINE}Light Status Grid (Offline, Thread, Case, Subscribed):']

        for hub in ["A", "B", "C", "D"]:
            line = f'\r{self.AnsiControl.ERASE_LINE} {hub}: '
            for i in range(16):
                state = self._device_states[hub][i]
                if state:
                    _, sessions, subs, light_on, attached = state
                    char = "T" if attached else "O"
                    if sessions > 1: char = "C"
                    if subs > 0: char = "S"
                    bg = self.AnsiBack.CYAN if light_on else self.AnsiBack.MAGENTA
                    fg = self.AnsiFore.RED if char != "S" else ""
                    line += self._colorize(char, bg, fg)
                else:
                    line += self._colorize('X', self.AnsiBack.WHITE)
            lines.append(line)

        unmapped_line = f'\r{self.AnsiControl.ERASE_LINE} -: '
        unmapped_ids = ""
        for port, state in self._device_states[None].items():
            if state:
                _, sessions, subs, light_on, attached = state
                char = "T" if attached else "O"
                if sessions > 1: char = "C"
                if subs > 0: char = "S"
                bg = self.AnsiBack.CYAN if light_on else self.AnsiBack.MAGENTA
                fg = self.AnsiFore.RED if char != "S" else ""
                unmapped_line += self._colorize(char, bg, fg)
                # Adding the node_id of unmapped devices to status line,  so we can map them
                node_id = state[0]
                unmapped_ids += f" {self._colorize(node_id, bg, fg)} "
        lines.append(unmapped_line + " - " + unmapped_ids)

        lines.append(f'\r{self.AnsiControl.ERASE_LINE}{status}\n')
        sys.stdout.write('\n'.join(lines))
        sys.stdout.flush()
        self._first_run = False

    def run_display_loop(self):
        """Hides cursor and enters the main render loop."""
        try:
            sys.stdout.write(self.AnsiControl.HIDE_CURSOR)
            while True:
                self.render(status="Press Ctrl-C to exit.")
                time.sleep(1)
        finally:
            sys.stdout.write('\n' * self._TOTAL_OUTPUT_LINES)
            sys.stdout.write(self.AnsiControl.SHOW_CURSOR)


def main():
    """Main function to discover devices and display their status."""
    port_prefix = '/dev/ttyACM' #TODO probably pass in as args

    def my_log_callback(log_bytes: bytes):
        """A simple callback to print log messages."""
        # TODO We can pipe these device logs to a file
        try:
            message = log_bytes.decode('utf-8', errors='replace').strip()
            if message:
                _LOG.debug("Device Log: %s", message)
        except UnicodeDecodeError:
            if log_bytes:
                _LOG.debug("Device Log (raw): %s", log_bytes)

    _LOG.info("Starting discovery...")
    discoverer = PigweedRpcDeviceDiscoverer(log_callback=my_log_callback)
    discovered_devices = discoverer.discover(port_prefix, timeout_s=10.0)

    if discovered_devices:
        _LOG.info("Discovered %d device(s): %s",
                  len(discovered_devices), [d.port for d in discovered_devices])
        ui = LightGrid()

        for device in discovered_devices:
            try:
                device.monitor(ui.update_device_state, interval_s=2.0)
            except Exception as e:
                _LOG.error("Could not get initial state or start monitor for %s: %s", device.port, e)

        try:
            ui.run_display_loop()
        except KeyboardInterrupt:
            _LOG.info("Keyboard interrupt received. Shutting down.")
        finally:
            _LOG.info("Stopping device monitors...")
            for device in discovered_devices:
                device.close()
            _LOG.info("All monitors stopped. Exiting.")
    else:
        _LOG.info("No devices found.")

if __name__ == '__main__':
    main()
