/**
 * Streaming parser & writer for mbox (RFC 4155) mail archives.
 * Designed to process arbitrarily large files (gigabytes) in the browser
 * using constant memory (~2-4 MB) via chunked slicing.
 */

export interface MboxMessage {
  /** The raw RFC 822 / .eml message content as a Uint8Array */
  bytes: Uint8Array;
  /** Estimated byte offset where this message started in the source file */
  offset: number;
  /** Sequential message index (1-based) */
  index: number;
  /** Whether the message indicates it belongs in Inbox according to Gmail labels */
  isInbox?: boolean;
}

/**
 * Searches for needle inside buffer, starting at fromIndex.
 */
function findSubarray(buffer: Uint8Array, needle: Uint8Array, fromIndex = 0): number {
  if (needle.length === 0 || buffer.length < needle.length) return -1;
  const max = buffer.length - needle.length;
  const first = needle[0];
  for (let i = fromIndex; i <= max; i++) {
    if (buffer[i] === first) {
      let match = true;
      for (let j = 1; j < needle.length; j++) {
        if (buffer[i + j] !== needle[j]) {
          match = false;
          break;
        }
      }
      if (match) return i;
    }
  }
  return -1;
}

/**
 * Unescapes mbox ">From " quote lines in-place or returns cleaned buffer.
 * RFC 4155 specifies that body lines starting with "From " are escaped as ">From ".
 */
function cleanMboxMessage(rawBytes: Uint8Array): Uint8Array {
  const pattern = new TextEncoder().encode('\n>From ');
  let idx = findSubarray(rawBytes, pattern);
  if (idx === -1) return rawBytes;

  const result: number[] = [];
  let last = 0;
  while (idx !== -1) {
    for (let i = last; i <= idx; i++) {
      result.push(rawBytes[i]);
    }
    // Skip the '>' character at idx + 1
    last = idx + 2;
    idx = findSubarray(rawBytes, pattern, last);
  }
  for (let i = last; i < rawBytes.length; i++) {
    result.push(rawBytes[i]);
  }
  return new Uint8Array(result);
}

/**
 * Checks headers for X-Gmail-Labels containing 'Inbox'
 */
function checkIsInbox(bytes: Uint8Array): boolean {
  const headerSlice = bytes.subarray(0, Math.min(bytes.length, 2048));
  const text = new TextDecoder().decode(headerSlice);
  const match = text.match(/X-Gmail-Labels:[^\r\n]+/i);
  if (!match) return false;
  return /\b(Inbox|INBOX)\b/i.test(match[0]);
}

/**
 * Strips leading envelope header line ("From <addr> <date>\r?\n") if present.
 */
function stripEnvelopeHeader(bytes: Uint8Array): Uint8Array {
  const fromStart = new TextEncoder().encode('From ');
  if (findSubarray(bytes, fromStart, 0) === 0) {
    const eol = bytes.indexOf(10); // '\n'
    if (eol !== -1 && eol + 1 < bytes.length) {
      return bytes.subarray(eol + 1);
    }
  }
  return bytes;
}

/**
 * Asynchronously stream-parses an mbox File or Blob.
 * Yields messages one at a time while maintaining a fixed read buffer.
 */
export async function* streamMbox(
  file: File | Blob,
  chunkSize = 2 * 1024 * 1024,
  onProgress?: (bytesRead: number, totalBytes: number) => void
): AsyncGenerator<MboxMessage, void, void> {
  const totalBytes = file.size;
  let fileOffset = 0;
  let carryOver: Uint8Array<ArrayBuffer> = new Uint8Array(0);

  const fromDelimLf = new TextEncoder().encode('\nFrom ');
  const fromDelimCrLf = new TextEncoder().encode('\r\nFrom ');
  const fromStart = new TextEncoder().encode('From ');

  let messageIndex = 0;
  let messageStartOffset = 0;
  let currentMsgParts: Uint8Array[] = [];

  while (fileOffset < totalBytes || carryOver.length > 0) {
    const nextReadSize = Math.min(chunkSize, totalBytes - fileOffset);
    let chunkBytes: Uint8Array;

    if (nextReadSize > 0) {
      const slice = file.slice(fileOffset, fileOffset + nextReadSize);
      chunkBytes = new Uint8Array(await slice.arrayBuffer());
      fileOffset += nextReadSize;
      if (onProgress) onProgress(fileOffset, totalBytes);
    } else {
      chunkBytes = new Uint8Array(0);
    }

    let buffer: Uint8Array<ArrayBuffer>;
    if (carryOver.length > 0 && chunkBytes.length > 0) {
      buffer = new Uint8Array(carryOver.length + chunkBytes.length);
      buffer.set(carryOver, 0);
      buffer.set(chunkBytes, carryOver.length);
      carryOver = new Uint8Array(0);
    } else if (carryOver.length > 0) {
      buffer = carryOver;
      carryOver = new Uint8Array(0);
    } else {
      buffer = chunkBytes as Uint8Array<ArrayBuffer>;
    }

    if (buffer.length === 0) break;

    let searchPos = 0;

    // Handle very first "From " line at byte 0 of file
    if (messageIndex === 0 && currentMsgParts.length === 0 && searchPos === 0) {
      if (findSubarray(buffer, fromStart, 0) === 0) {
        const eol = buffer.indexOf(10); // '\n'
        if (eol !== -1) {
          searchPos = eol + 1;
        } else if (fileOffset < totalBytes) {
          carryOver = buffer.slice();
          continue;
        }
      }
    }

    while (searchPos < buffer.length) {
      let delimLen = 6;
      let nextDelim = findSubarray(buffer, fromDelimLf, searchPos);
      const crlfDelim = findSubarray(buffer, fromDelimCrLf, searchPos);

      if (crlfDelim !== -1 && (nextDelim === -1 || crlfDelim < nextDelim)) {
        nextDelim = crlfDelim;
        delimLen = 7;
      }

      if (nextDelim !== -1) {
        const msgEnd = nextDelim;
        const part = buffer.subarray(searchPos, msgEnd);
        if (part.length > 0) currentMsgParts.push(part);

        const totalMsgLen = currentMsgParts.reduce((acc, p) => acc + p.length, 0);
        const fullMsg = new Uint8Array(totalMsgLen);
        let ptr = 0;
        for (const p of currentMsgParts) {
          fullMsg.set(p, ptr);
          ptr += p.length;
        }
        currentMsgParts = [];

        messageIndex++;
        const cleaned = cleanMboxMessage(stripEnvelopeHeader(fullMsg));
        yield {
          bytes: cleaned,
          offset: messageStartOffset,
          index: messageIndex,
          isInbox: checkIsInbox(cleaned),
        };

        const envelopeLineStart = nextDelim + delimLen;
        const envelopeEol = buffer.indexOf(10, envelopeLineStart);
        if (envelopeEol !== -1) {
          searchPos = envelopeEol + 1;
          messageStartOffset = fileOffset - buffer.length + searchPos;
        } else {
          // Envelope line continues into next chunk
          if (fileOffset < totalBytes) {
            carryOver = buffer.slice(nextDelim + (delimLen - 5)); // back up to 'From '
          } else {
            searchPos = buffer.length;
          }
          break;
        }
      } else {
        // No more delimiters in this buffer
        if (fileOffset < totalBytes) {
          const keep = Math.min(8, buffer.length - searchPos);
          const safeEnd = buffer.length - keep;
          if (safeEnd > searchPos) {
            currentMsgParts.push(buffer.subarray(searchPos, safeEnd));
            searchPos = safeEnd;
          }
          carryOver = buffer.slice(searchPos);
        } else {
          // Reached EOF of file
          currentMsgParts.push(buffer.subarray(searchPos));
          searchPos = buffer.length;
          carryOver = new Uint8Array(0);
        }
        break;
      }
    }
  }

  // Final remaining message if any
  if (currentMsgParts.length > 0) {
    const totalMsgLen = currentMsgParts.reduce((acc, p) => acc + p.length, 0);
    if (totalMsgLen > 0) {
      const fullMsg = new Uint8Array(totalMsgLen);
      let ptr = 0;
      for (const p of currentMsgParts) {
        fullMsg.set(p, ptr);
        ptr += p.length;
      }
      messageIndex++;
      const cleaned = cleanMboxMessage(stripEnvelopeHeader(fullMsg));
      yield {
        bytes: cleaned,
        offset: messageStartOffset,
        index: messageIndex,
        isInbox: checkIsInbox(cleaned),
      };
    }
  }
}

/**
 * Formats a message into mbox envelope line and escapes inner `From ` lines.
 */
export function formatMboxEntry(rawEml: Uint8Array, date = new Date(), fromAddress = 'MAILER-DAEMON'): Uint8Array {
  // Format RFC 4155 envelope header: "From MAILER-DAEMON Mon Jan 01 00:00:00 2024\r\n"
  const dateStr = date.toUTCString();
  const header = new TextEncoder().encode(`From ${fromAddress} ${dateStr}\r\n`);

  // Escape lines starting with "From " -> ">From "
  const text = new TextDecoder().decode(rawEml);
  const escaped = text.replace(/(^|\n)(>*From )/g, '$1>$2');
  const bodyBytes = new TextEncoder().encode(escaped + '\r\n\r\n');

  const result = new Uint8Array(header.length + bodyBytes.length);
  result.set(header, 0);
  result.set(bodyBytes, header.length);
  return result;
}
