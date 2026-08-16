'use strict'
Object.defineProperty(exports, '__esModule', { value: true })
exports.USyncQuery = void 0
const WABinary_1 = require('../WABinary')
const USyncBotProfileProtocol_1 = require('./Protocols/USyncBotProfileProtocol')
const USyncLIDProtocol_1 = require('./Protocols/USyncLIDProtocol')
const USyncBusinessProtocol_1 = require('./Protocols/USyncBusinessProtocol')
const USyncFeatureProtocol_1 = require('./Protocols/USyncFeatureProtocol')
const USyncPictureProtocol_1 = require('./Protocols/USyncPictureProtocol')
const USyncTextStatusProtocol_1 = require('./Protocols/USyncTextStatusProtocol')
const USyncSidelistProtocol_1 = require('./Protocols/USyncSidelistProtocol')
const Protocols_1 = require('./Protocols')
class USyncQuery {
	constructor() {
		this.protocols = []
		this.users = []
		this.context = 'interactive'
		this.mode = 'query'
	}
	withMode(mode) {
		this.mode = mode
		return this
	}
	withContext(context) {
		this.context = context
		return this
	}
	withUser(user) {
		this.users.push(user)
		return this
	}
	parseUSyncQueryResult(result) {
		if (!result || result.attrs.type !== 'result') {
			return
		}
		const protocolMap = Object.fromEntries(
			this.protocols.map(protocol => {
				return [protocol.name, protocol.parser]
			})
		)
		const queryResult = {
			errors: {},
			refresh: {},
			list: [],
			sideList: []
		}
		const usyncNode = (0, WABinary_1.getBinaryNodeChild)(result, 'usync')
		
		
		
		
		const resultNode = usyncNode ? (0, WABinary_1.getBinaryNodeChild)(usyncNode, 'result') : undefined
		if (resultNode) {
			for (const protocol of this.protocols) {
				const protocolNode = (0, WABinary_1.getBinaryNodeChild)(resultNode, protocol.name)
				if (!protocolNode) continue
				const errorNode = (0, WABinary_1.getBinaryNodeChild)(protocolNode, 'error')
				if (errorNode) {
					queryResult.errors[protocol.name] = {
						errorCode: errorNode.attrs.code ? +errorNode.attrs.code : undefined,
						errorText: errorNode.attrs.text,
						errorBackoff: errorNode.attrs.backoff ? +errorNode.attrs.backoff : undefined
					}
				} else if (protocolNode.attrs.refresh !== undefined) {
					queryResult.refresh[protocol.name] = +protocolNode.attrs.refresh
				}
			}
		}
		const parseUserNodes = nodes => {
			return nodes.reduce((acc, node) => {
				const id = node?.attrs.jid
				if (id) {
					
					
					const userErrorCode = node?.attrs?.error ? parseInt(node.attrs.error, 10) : 0
					const isBlockedByAttr = userErrorCode === 401 || userErrorCode === 403 || userErrorCode === 405

					const data = Array.isArray(node?.content)
						? Object.fromEntries(
								node.content
									.map(content => {
										const protocol = content.tag

										
										
										if (protocol === 'privacy') {
											const tokenNode = (0, WABinary_1.getBinaryNodeChild)(content, 'token')
											const modeTsNode = (0, WABinary_1.getBinaryNodeChild)(content, 'mode_ts')
											
											const tokenVal = tokenNode?.content || content.attrs?.token
											const modeTsVal =
												modeTsNode?.content?.toString?.() || modeTsNode?.attrs?.value || content.attrs?.mode_ts || null
											if (tokenVal) {
												return [
													'privacy',
													{
														token:
															Buffer.isBuffer(tokenVal) || tokenVal instanceof Uint8Array
																? Buffer.from(tokenVal)
																: tokenVal,
														modeTs: modeTsVal
													}
												]
											}
											return ['privacy', null]
										}

										const parser = protocolMap[protocol]
										if (parser) {
											try {
												return [protocol, parser(content)]
											} catch (err) {
												
												
												const errCode = err?.data ?? err?.output?.payload?.data
												if (errCode === 401 || errCode === 403 || errCode === 405) {
													return ['isBlockedByContact', true]
												}
												throw err
											}
										} else {
											return [protocol, null]
										}
									})
									.filter(([, b]) => b !== null)
							)
						: {}

					
					if (isBlockedByAttr) {
						data.isBlockedByContact = true
					}

					
					if ('disappearing_mode' in data) {
						data.disappearingMode = data.disappearing_mode
						delete data.disappearing_mode
					}

					acc.push({ ...data, id })
				}
				return acc
			}, [])
		}
		const listNode = usyncNode ? (0, WABinary_1.getBinaryNodeChild)(usyncNode, 'list') : undefined
		if (listNode?.content && Array.isArray(listNode.content)) {
			queryResult.list = parseUserNodes(listNode.content)
		}
		const sideListNode = usyncNode ? (0, WABinary_1.getBinaryNodeChild)(usyncNode, 'side_list') : undefined
		if (sideListNode?.content && Array.isArray(sideListNode.content)) {
			queryResult.sideList = parseUserNodes(sideListNode.content)
		}
		return queryResult
	}
	withDeviceProtocol() {
		this.protocols.push(new Protocols_1.USyncDeviceProtocol())
		return this
	}
	withContactProtocol() {
		this.protocols.push(new Protocols_1.USyncContactProtocol())
		return this
	}
	withStatusProtocol() {
		this.protocols.push(new Protocols_1.USyncStatusProtocol())
		return this
	}
	withDisappearingModeProtocol() {
		this.protocols.push(new Protocols_1.USyncDisappearingModeProtocol())
		return this
	}
	withBotProfileProtocol() {
		this.protocols.push(new USyncBotProfileProtocol_1.USyncBotProfileProtocol())
		return this
	}
	withLIDProtocol() {
		this.protocols.push(new USyncLIDProtocol_1.USyncLIDProtocol())
		return this
	}
	withUsernameProtocol() {
		this.protocols.push(new Protocols_1.USyncUsernameProtocol())
		return this
	}
	withBusinessProtocol(profileVersion) {
		this.protocols.push(new USyncBusinessProtocol_1.USyncBusinessProtocol(profileVersion))
		return this
	}
	withPictureProtocol(type) {
		this.protocols.push(new USyncPictureProtocol_1.USyncPictureProtocol(type))
		return this
	}
	withTextStatusProtocol() {
		this.protocols.push(new USyncTextStatusProtocol_1.USyncTextStatusProtocol())
		return this
	}
	withSidelistProtocol(useLidAddressing) {
		this.protocols.push(new USyncSidelistProtocol_1.USyncSidelistProtocol(useLidAddressing))
		return this
	}
	withFeatureProtocol(features) {
		this.protocols.push(new USyncFeatureProtocol_1.USyncFeatureProtocol(features))
		return this
	}
}
exports.USyncQuery = USyncQuery
