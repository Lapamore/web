import { Component } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';
import { UserService } from '../user/service';

@Component({
  selector: 'app-login-dialog',
  template: `
    <h2 mat-dialog-title>Вход в систему</h2>
    <mat-dialog-content class="mat-typography">
      <form [formGroup]="loginForm" (ngSubmit)="onLogin()">
        <mat-form-field appearance="fill" class="full-width">
          <mat-label>Логин</mat-label>
          <input matInput formControlName="username" required>
          <mat-error *ngIf="loginForm.get('username')?.hasError('required')">
            Логин обязателен
          </mat-error>
        </mat-form-field>

        <mat-form-field appearance="fill" class="full-width">
          <mat-label>Пароль</mat-label>
          <input matInput type="password" formControlName="password" required>
          <mat-error *ngIf="loginForm.get('password')?.hasError('required')">
            Пароль обязателен
          </mat-error>
        </mat-form-field>

        <div *ngIf="errorMessage" class="error-message">
          {{ errorMessage }}
        </div>
      </form>
    </mat-dialog-content>
    <mat-dialog-actions align="end">
      <button mat-button mat-dialog-close>Отмена</button>
      <button mat-raised-button color="primary" 
              [disabled]="loginForm.invalid" 
              (click)="onLogin()">
        Войти
      </button>
    </mat-dialog-actions>
  `,
  styles: [`
    .full-width {
      width: 100%;
      margin-bottom: 16px;
    }
    .error-message {
      color: #f44336;
      font-size: 14px;
      margin-top: 8px;
    }
    mat-dialog-content {
      min-width: 300px;
      padding: 20px;
    }
  `]
})
export class LoginDialogComponent {
  loginForm: FormGroup;
  errorMessage: string = '';

  constructor(
    private fb: FormBuilder,
    private userService: UserService,
    public dialogRef: MatDialogRef<LoginDialogComponent>
  ) {
    this.loginForm = this.fb.group({
      username: ['', Validators.required],
      password: ['', Validators.required]
    });
  }

  onLogin(): void {
    if (this.loginForm.valid) {
      const { username, password } = this.loginForm.value;
      const result = this.userService.login(username, password);
      
      if (result.success) {
        this.dialogRef.close(true);
      } else {
        this.errorMessage = result.message || 'Ошибка входа';
      }
    }
  }
}