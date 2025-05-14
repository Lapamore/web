import { Component, ChangeDetectionStrategy } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { MessageService } from '../message.service';

@Component({
  selector: 'app-messages',
  templateUrl: './messages.component.html',
  styleUrls: ['./messages.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MessagesComponent {
  messages$: Observable<string[]>;
  hasMessages$: Observable<boolean>;

  constructor(public messageService: MessageService) {
    this.messages$ = messageService.messages$;
    this.hasMessages$ = this.messages$.pipe(
      map(messages => messages.length > 0)
    );
  }

  clearMessages(): void {
    this.messageService.clear();
  }
}
