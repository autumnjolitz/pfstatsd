import logging

logger = logging.getLogger(__name__)


class ProtocolStateMachine:
    CONNECTED_STATES = frozenset(('connection_made', 'data_received', 'eof_received'))
    STATES = {
        'not_connected': frozenset(('connection_made',)),
        'connection_made': frozenset(('data_received', 'eof_received', 'connection_lost',)),
        'data_received': frozenset(('data_received', 'eof_received', 'connection_lost',)),
        'eof_received': frozenset(('connection_lost',)),
        'connection_lost': frozenset(('connection_made',)),
    }

    def __init__(self, *args, **kwargs):
        self._state_generation = 0
        self._current_state = None
        self._next_allowed_states = frozenset(('not_connected',))

        super().__init__(*args, **kwargs)
        self.current_state = 'not_connected'

    @property
    def current_state(self):
        return self._current_state

    @current_state.setter
    def current_state(self, desired_state):
        assert desired_state in self._next_allowed_states, \
            f'{desired_state} not in {self._next_allowed_states}, currently {self._current_state}'

        logger.debug(
            f'Changing StateMachine to {self._current_state}->{desired_state}, '
            'allowed hops {{{}}}'.format(', '.join(self.__class__.STATES[desired_state])))
        self._current_state = desired_state
        self._next_allowed_states = self.__class__.STATES[desired_state]

    def connection_made(self, transport):
        self.current_state = 'connection_made'
        super().connection_made(transport)

    def data_received(self, data):
        self.current_state = 'data_received'
        super().data_received(data)

    def eof_received(self):
        self.current_state = 'eof_received'
        super().eof_received()

    def connection_lost(self, exc):
        self.current_state = 'connection_lost'
        super().connection_lost(exc)
