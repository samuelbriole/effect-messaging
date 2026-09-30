# Effect Messaging

Language for messaging through recoverable AMQP connections and channels.

## AMQP

**Logical connection**:
A connection identity that remains available across transient transport failures.
_Avoid_: Socket, session

**Connection session**:
One negotiated AMQP conversation over a transport connection.
_Avoid_: Epoch

**Logical channel**:
A channel identity whose configuration and desired topology survive replacement of its connection session.

**Channel session**:
One numbered channel within a particular connection session.
_Avoid_: Physical

**Desired topology**:
The declarations, bindings, and consumer registrations that should exist after recovery.
_Avoid_: Command history, replay log

**Queue reference**:
A stable queue identity whose current broker-assigned name may change during recovery.
_Avoid_: Queue name

**Delivery settlement**:
An acknowledgement, negative acknowledgement, or rejection of a received delivery on its originating channel session.
_Avoid_: Publisher confirmation

**Publisher confirmation**:
The broker's acknowledgement or negative acknowledgement of a published message.
_Avoid_: Delivery settlement, consumer acknowledgement
