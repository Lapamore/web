import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class MessageService {
  private _messages = new BehaviorSubject<string[]>([]);
  messages$: Observable<string[]> = this._messages.asObservable();

  // Для обратной совместимости
  get messages(): string[] {
    return this._messages.value;
  }

  add(message: string) {
    const currentMessages = this._messages.value;
    this._messages.next([...currentMessages, message]);
  }

  clear() {
    this._messages.next([]);
  }
}
