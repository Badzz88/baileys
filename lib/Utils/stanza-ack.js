'use strict'
Object.defineProperty(exports, '__esModule', { value: true })
exports.buildAckStanza = buildAckStanza


const CALL_STANZA_TAGS = new Set([
	'offer',
	'offer_notice',
	'terminate',
	'accept',
	'reject',
	'preaccept',
	'transport',
	'video',
	'duration',
	'mute_v2',
	'lobby',
	'heartbeat',
	'relaylatency',
	'link_query',
	'group_update',
	'accept_ack',
	'enc-rekey',
	'enc_rekey',
	'peer_state',
	'group_info',
	'video_state',
	'video_state_ack',
	'flow_control',
	'waiting_room_request'
])

function buildAckStanza(node, errorCode, meId) {
	const { tag, attrs } = node
	const isCallStanza = CALL_STANZA_TAGS.has(tag)
	const stanza = {
		tag: 'ack',
		attrs: {
			id: attrs.id,
			to: attrs.from,
			class: isCallStanza ? 'call' : tag
		}
	}
	
	if (isCallStanza) {
		stanza.attrs.type = tag
	}
	if (errorCode) {
		stanza.attrs.error = errorCode.toString()
	}
	if (attrs.participant) {
		stanza.attrs.participant = attrs.participant
	}
	if (attrs.recipient) {
		stanza.attrs.recipient = attrs.recipient
	}
	
	
	if (attrs.type && !isCallStanza) {
		stanza.attrs.type = attrs.type
	}
	
	if (tag === 'message' && meId) {
		stanza.attrs.from = meId
	}
	return stanza
}
